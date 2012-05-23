package org.basex.dist;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;

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
  /** lock. */
  public Lock connectionLock;
  /** wait to be connected. */
  public Condition connected;
  /** XQuery to be executed. */
  public String xquery;
  /** Lock to execute only one xquery at a time. */
  public Lock xqueryLock;

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
    connectionLock = new ReentrantLock();
    connected = connectionLock.newCondition();
    xqueryLock = new ReentrantLock();

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
    nodes = old.nodes;
    log = old.log;
    ctx = old.ctx;
  }

  /**
   * Adds a new peer to the network cluster. This is just used for the internal table of
   * this peer and has no effect on the recognition of this peer in the overall network.
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
      ClusterPeer newPeer = new ClusterPeer(this, host, port +
            nodes.values().size() + 2, cHost, cPort, false);
      newPeer.actionType = DistConstants.action.SIMPLE_CONNECT;
      new Thread(newPeer).start();
      newPeer.actionLock.lock();
      newPeer.action.signalAll();
      newPeer.actionLock.unlock();

      newPeer.connectionLock.lock();
      try {
        newPeer.connected.await();
      } catch(InterruptedException e) {
        log.write("Interrupt exception");
        return false;
      } finally {
        newPeer.connectionLock.unlock();
      }

      if (newPeer.getStatus() == DistConstants.status.CONNECTED) {
        addPeerToNetwork(newPeer);
        return true;
      }

      return false;
    } catch (IOException e) {
      log.write("Could not create I/O on the socket.");
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
  public boolean connectTo(final InetAddress cHost, final int cPort) {
    try {
      ClusterPeer newPeer = new ClusterPeer(this, host, port +
            nodes.values().size() + 1, cHost, cPort, true);

      newPeer.actionType = DistConstants.action.FIRST_CONNECT;
      new Thread(newPeer).start();
      newPeer.actionLock.lock();
      newPeer.action.signalAll();
      newPeer.actionLock.unlock();

      newPeer.connectionLock.lock();
      try {
        newPeer.connected.await();
      } catch(InterruptedException e) {
        log.write("Interrupt exception");
        return false;
      } finally {
        newPeer.connectionLock.unlock();
      }

      return true;
    } catch (IOException e) {
      log.write("Could not create I/O on the socket.");
      return false;
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

  /**
   * Executes the provided XQuery on all connected peers.
   * @param q XQuery to execute.
   */
  public void executeXQuery(final String q) {
    xquery = q;

    Iterator<ClusterPeer> i = nodes.values().iterator();
    while (i.hasNext()) {
      ClusterPeer cp = i.next();
      cp.actionType = DistConstants.action.XQUERY;

      cp.actionLock.lock();
      cp.action.signalAll();
      cp.actionLock.unlock();
    }
  }

  @Override
  public void run() {
    while (running) {
      try {
        /* Waiting for another node to connect */
        try {
          socketIn = serverSocket.accept();
          socketIn.setReuseAddress(true);
        } catch(SocketTimeoutException e) {
          continue;
        }

        ClusterPeer cn = new ClusterPeer(this, socketIn, false);
        Thread t = new Thread(cn);
        t.start();
      } catch(IOException e) {
        log.write("I/O Exception");
        running = false;
      }
    }

    close();
  }
}