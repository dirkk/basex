package org.basex.dist;

import java.io.*;
import java.net.*;
import java.util.*;

import org.basex.core.*;
import org.basex.server.*;

/**
 * This is the starter class, to open a BaseX instance as a network node, using a
 * peer-to-peer infrastructure.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class NetworkPeer implements Runnable {
  /** Server socket. */
  protected ServerSocket serverSocket;
  /** listening socket. */
  protected Socket socketIn;
  /** data input stream. */
  protected DataInputStream in;
  /** data output stream. */
  protected DataOutputStream out;
  /** already running? */
  public volatile boolean running;
  /** List of connected nodes. */
  protected Map<String, ClusterPeer> nodes;
  /** log file. */
  protected Log log;
  /** host name. */
  protected final InetAddress host;
  /** port number. */
  protected final int port;
  /** database context. */
  protected final Context ctx;
  /** Reference to the super-peer, null if super-peer itself. */
  protected ClusterPeer superPeer;
  /** Host this peer should connect to. */
  protected InetAddress connectHost;
  /** port number this peer should connect to. */
  protected int connectPort;

  /**
   * Makes this instance of BaseX to a node in a clustered BaseX network.
   *
   * @param nHost listening hostname
   * @param nPort port number
   * @param context The database context.
   * @throws UnknownHostException unknown host name.
   */
  public NetworkPeer(final String nHost, final int nPort, final Context context)
      throws UnknownHostException {
    ctx = context;
    log = new Log(ctx, false);

    port = nPort;

    // open socket for incoming packets
    host = nHost.isEmpty() ? null : InetAddress.getByName(nHost);

    try {
      serverSocket = new ServerSocket();
      serverSocket.setReuseAddress(true);
      serverSocket.setSoTimeout(5000);
      serverSocket.bind(new InetSocketAddress(host, port));
      running = true;
    } catch(IOException e) {
      log.write("Error while binding socket.");
    }

    nodes = new LinkedHashMap<String, ClusterPeer>();
  }

  /**
   * Create a new instance from an already existing one.
   *
   * @param old Old network peer
   */
  public NetworkPeer(final NetworkPeer old) {
    host = old.host;
    port = old.port;
    serverSocket = old.serverSocket;
    running = old.running;
    socketIn = old.socketIn;
    in = old.in;
    out = old.out;
    nodes = old.nodes;
    log = old.log;
    ctx = old.ctx;
  }

  /**
   * Sets the address of the peer to connect to.
   *
   * @param cHost host to connect to
   * @param cPort port number to connect to
   * @throws UnknownHostException  unknown host
   */
  public void setConnectHost(final String cHost, final int cPort)
      throws UnknownHostException {
    connectHost = InetAddress.getByName(cHost);
    connectPort = cPort;
  }

  @Override
  public void run() {
    connectTo(connectHost, connectPort);

    while (running) {
      try {
        /* Waiting for another node to connect */
        try {
          socketIn = serverSocket.accept();
          socketIn.setReuseAddress(true);
        } catch(SocketTimeoutException e) {
          continue;
        }

        ClusterPeer cn = new ClusterPeer(this, socketIn);
        Thread t = new Thread(cn);
        t.start();

        cn.doHandleConnect = true;
        cn.lock.lock();
        cn.action.signalAll();
        cn.lock.unlock();
      } catch(IOException e) {
        log.write("I/O Exception");
        running = false;
      }
    }

    close();
  }

  /**
   * Adds a new node to the network cluster. This is just used for the internal table of
   * this node and has no effect on the recognition of this node in the overall network.
   * @param cp The peer to add
   */
  protected void addPeerToNetwork(final ClusterPeer cp) {
    nodes.put(cp.getIdentifier(), cp);
    cp.changeStatus(DistConstants.status.CONNECTED);
  }

  /**
   * Connects this peer with the given normal peer.
   *
   * @param cHost The host name to connect to
   * @param cPort The port number to connect to.
   * @return success.
   */
  public boolean connectToPeer(final InetAddress cHost, final int cPort) {
    try {
      Socket s = new Socket(cHost, cPort, host, port + nodes.values().size() + 2);
      s.setReuseAddress(true);
      ClusterPeer newPeer = new ClusterPeer(this, s);
      out = new DataOutputStream(s.getOutputStream());
      DataInputStream inNow = new DataInputStream(s.getInputStream());

      out.write(DistConstants.P_CONNECT);
      if (inNow.readByte() == DistConstants.P_CONNECT_ACK) {
        newPeer.doSimpleConnect = true;
        new Thread(newPeer).start();
        newPeer.lock.lock();
        newPeer.action.signalAll();
        newPeer.lock.unlock();
        return true;
      }

      return false;
    } catch (IOException e) {
      log.write("Could not connect to peer " + cHost.getHostAddress());
      return false;
    }
  }

  /**
   * Join an already established cluster of a BaseX network. Connect to any one of the
   * peers.
   *
   * @param cHost the name of the node to connect to.
   * @param cPort the port number of the node to connect to.
   */
  protected void connectTo(final InetAddress cHost, final int cPort) {
    try {
      Socket socketOut = new Socket(cHost, cPort, host, port + nodes.values().size() + 1);
      socketOut.setReuseAddress(true);
      out = new DataOutputStream(socketOut.getOutputStream());
      DataInputStream inNow = new DataInputStream(socketOut.getInputStream());

      out.write(DistConstants.P_CONNECT);

      byte packetIn = inNow.readByte();
      if(packetIn == DistConstants.P_CONNECT_ACK) {
        superPeer = new ClusterPeer(this, socketOut, true);
        superPeer.doFirstConnect = true;
        new Thread(superPeer).start();
        superPeer.lock.lock();
        superPeer.action.signalAll();
        superPeer.lock.unlock();
      } else if (packetIn == DistConstants.P_SUPERPEER_ADDR) {
          int length = inNow.readInt();
          byte[] nHost = new byte[length];
          inNow.read(nHost, 0, length);
          connectTo(InetAddress.getByAddress(nHost), inNow.readInt());
        }
    } catch(BindException e) {
      log.write("Could not bind to this address.");
      log.write(e.getMessage());
    } catch(IOException e) {
      log.write("I/O error while trying to connect to the node cluster.");
    }
  }

  /**
   * Shuts down this network node.
   *
   */
  public void close() {
    // send a message to notify of the disconnect to all connected nodes
    // free resources
    try {
      if (serverSocket != null && serverSocket.isBound()) {
        serverSocket.close();
      }
      if (socketIn != null && socketIn.isBound()) {
        socketIn.close();
      }
    } catch (IOException e) {
      log.write("Could not free a socket.");
    }
  }

  /**
   * Is this a super-peer itself?
   * @return boolean true, if super-peer.
   */
  public boolean isSuperPeer() {
    if(superPeer == null) return true;

    return false;
  }

  /**
   * Gives information about this peer and connected peers.
   * @return information about this peer.
   */
  public String info() {
    String o = new String("Normal peer\r\n");
    if (superPeer != null) {
      o += "Super-Peer: " + superPeer.socket.getInetAddress().toString() + ":" +
          superPeer.socket.getPort() + "\r\n";
    } else {
      o += "Super-Peer: No super-peer registered.\r\n";
    }

    for(ClusterPeer c : nodes.values()) {
      o += "|--- " + c.getIdentifier() + "\r\n";
    }

    return o;
  }

  /**
   * Returns a unique identifier for this peer.
   *
   * @return A unique identifier.
   */
  public String getIdentifier() {
    return host.getHostAddress() + ":" + port;
  }
}