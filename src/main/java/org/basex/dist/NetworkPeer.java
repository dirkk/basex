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
  /** socket for outgoing packets. */
  protected Socket socketOut;
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
  /** connections status of this node. */
  protected DistConstants.status status;
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
    status = DistConstants.status.DISCONNECTED;

    // open socket for incoming packets
    host = nHost.isEmpty() ? null : InetAddress.getByName(nHost);

    try {
      serverSocket = new ServerSocket();
      serverSocket.setReuseAddress(false);
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
    status = old.status;
    serverSocket = old.serverSocket;
    running = old.running;
    socketIn = old.socketIn;
    in = old.in;
    socketOut = old.socketOut;
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
    running = false;

    while(running) {
      try {
        /* Waiting for another node to connect */
        try {
          socketIn = serverSocket.accept();
        } catch(SocketTimeoutException e) {
          continue;
        }
        in = new DataInputStream(socketIn.getInputStream());
        DataOutputStream cOut = new DataOutputStream(socketIn.getOutputStream());

        byte packetIn = in.readByte();
        if(packetIn == DistConstants.P_CONNECT) {
          String newHost = socketIn.getInetAddress().toString();
          int newPort = socketIn.getPort() - 1;
          ClusterPeer cn = new ClusterPeer(newHost, newPort);

          addNodeToNetwork(cn);
          cOut.write(DistConstants.P_CONNECT_ACK);

          if(in.readByte() == DistConstants.P_SUPERPEER_NEAREST) {
            cOut.write(DistConstants.P_SUPERPEER_ADDR);
            byte[] bHost = superPeer.getHostAsByte();
            cOut.writeInt(bHost.length);
            cOut.write(bHost, 0, bHost.length);
            cOut.writeInt(superPeer.port);
          }

        } else if(packetIn == DistConstants.P_CONNECT_SUPER) {
          cOut.write(DistConstants.P_CONNECT_ACK);

          if(packetIn == DistConstants.P_SUPERPEER_ADDR) {
            cOut.write(DistConstants.P_SUPERPEER_ADDR);
            byte[] bHost = superPeer.getHostAsByte();
            cOut.writeInt(bHost.length);
            cOut.write(bHost, 0, bHost.length);
            cOut.writeInt(superPeer.port);
          }
        }
      } catch(IOException e) {
        e.printStackTrace();
      }
    }

    close();
  }

  /**
   * Notify all connected nodes of a new peer.
   *
   * @param cn The new peer in the network.
   */
  protected void propagateNewPeer(final ClusterPeer cn) {
    /*
    for(ClusterPeer notifyPeer : nodes.values()) {
      if(!notifyPeer.getIdentifier().equalsIgnoreCase(cn.getIdentifier())) {
        try {
          socketOut = new Socket(notifyPeer.host, notifyPeer.port);
          socketOut.setReuseAddress(false);
          out = new DataOutputStream(socketOut.getOutputStream());

          DataInputStream inFromRemote = new DataInputStream(socketOut.getInputStream());
          out.write(DistConstants.P_NEW_NODE);
          byte[] bHost = cn.getHostAsByte();
          out.write(bHost.length);
          out.write(bHost);
          out.writeInt(cn.port);
          if(inFromRemote.readByte() != DistConstants.P_NEW_NODE_ACK) {
            // transmission seems to have failed
          }
        } catch(IOException e) {
          e.printStackTrace();
        }
      }
    }*/
  }

  /**
   * Adds a new node to the network cluster. This is just used for the internal table of
   * this node and has no effect on the recognotion of this node in the overall network.
   *
   * @param cn The node to add.
   */
  protected void addNodeToNetwork(final ClusterPeer cn) {
    cn.changeStatus(DistConstants.status.PENDING);
    nodes.put(cn.getIdentifier(), cn);
  }

  /**
   * Join an already established cluster of BaseX nodes. Connect to any one of the nodes.
   *
   * @param cHost the name of the node to connect to.
   * @param cPort the port number of the node to connect to.
   * @return Returns true or false, regarding if the connection was successful. However,
   *         even if true some nodes could be detected as missing. These nodes will then
   *         be removed from the cluster.
   */
  protected boolean connectTo(final InetAddress cHost, final int cPort) {
    // open socket (if needed) for outgoing packets
    if(socketOut == null || cHost != socketOut.getInetAddress()
        || cPort != socketOut.getPort()) {
      try {
        socketOut = new Socket(cHost, cPort, host, port + 1);
        socketOut.setReuseAddress(false);
        out = new DataOutputStream(socketOut.getOutputStream());
        DataInputStream inFromRemote = new DataInputStream(socketOut.getInputStream());

        out.write(DistConstants.P_CONNECT);

        if(inFromRemote.readByte() == DistConstants.P_CONNECT_ACK) {
          out.write(DistConstants.P_SUPERPEER_NEAREST);
          byte packetIn = inFromRemote.readByte();
          if(packetIn == DistConstants.P_SUPERPEER_ADDR) {
            int length = inFromRemote.readInt();
            byte[] nHost = new byte[length];
            inFromRemote.read(nHost, 0, length);
            superPeer = new ClusterPeer(InetAddress.getByAddress(nHost),
                inFromRemote.readInt(), true);

            socketOut = new Socket(superPeer.host, superPeer.port, host, port + 1);
            socketOut.setReuseAddress(false);
            out = new DataOutputStream(socketOut.getOutputStream());
            inFromRemote = new DataInputStream(socketOut.getInputStream());

            out.write(DistConstants.P_SUPERPEER_ADD_ME);

            if(inFromRemote.readByte() != DistConstants.P_CONNECT_ACK) {
              return false;
            }
          } else if(packetIn == DistConstants.P_SUPERPEER_ME) {
            superPeer = new ClusterPeer(cHost, cPort, true);
          } else {
            return false;
          }

          status = DistConstants.status.PENDING;

          if(inFromRemote.readByte() == DistConstants.P_CONNECT_NODES) {
            int nnodes = inFromRemote.readInt();
            for(int i = 0; i < nnodes; ++i) {
              int length = inFromRemote.readInt();
              byte[] nbHost = new byte[length];
              inFromRemote.read(nbHost, 0, length);
              InetAddress nHost = InetAddress.getByAddress(nbHost);
              int nPort = inFromRemote.readInt();

              ClusterPeer newPeer;
              if(nHost.equals(superPeer.host) && nPort == superPeer.port) {
                newPeer = new ClusterPeer(nHost, nPort, true);
                superPeer = newPeer;
              } else {
                newPeer = new ClusterPeer(nHost, nPort);
              }
              addNodeToNetwork(newPeer);
            }

            out.write(DistConstants.P_CONNECT_NODES_ACK);
            status = DistConstants.status.CONNECTED;
            return true;
          }

          return false;
        }
        return false;
      } catch(BindException e) {
        log.write("Could not bind to this address.");
        log.write(e.getMessage());
        return false;
      } catch(IOException e) {
        log.write("I/O error while trying to connect to the node cluster.");
        return false;
      }
    }

    return true;
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
      if (socketOut != null && socketOut.isBound()) {
        socketOut.close();
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
      o += "Super-Peer: " + superPeer.getIdentifier() + "\r\n";
    } else {
      o += "Super-Peer: No super-peer registered.\r\n";
    }

    o += "State: " + status.toString() + "\r\n";

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
