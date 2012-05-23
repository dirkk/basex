package org.basex.dist;

import java.io.*;
import java.net.*;
import java.util.concurrent.locks.*;

/**
 * Another node within the same cluster as this node.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 *
 */
public class ClusterPeer implements Runnable {
  /** Is this a super-peer? */
  protected boolean superPeer;
  /** Commanding network-peer. */
  protected NetworkPeer commandingPeer;
  /** status of this node. */
  protected DistConstants.status status;
  /** connection socket. */
  protected Socket socket;
  /** data input stream. */
  protected DataInputStream in;
  /** data output stream. */
  protected DataOutputStream out;
  /** is this peer still active? */
  protected boolean running;
  /** host name. */
  protected final InetAddress host;
  /** port number. */
  protected final int port;
  /** Host this peer should connect to. */
  protected InetAddress connectHost;
  /** port number this peer should connect to. */
  protected int connectPort;
  /** action lock. */
  public final Lock actionLock = new ReentrantLock();
  /** to notify of new events. */
  public Condition action = actionLock.newCondition();
  /** connection lock. */
  public final Lock connectionLock = new ReentrantLock();
  /** connect condition. */
  public Condition connected = connectionLock.newCondition();
  /** connect to a cluster. This is the first attempt, so request some information. */
  public DistConstants.action actionType;
  /** Listener for incoming requests from the other peer. */
  protected ClusterPeerListener listener;

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The socket to communicate to.
   * @param newSuperPeer is this a super-peer?
   * @throws IOException 
   */
  public ClusterPeer(final NetworkPeer c, final Socket s, final boolean newSuperPeer) throws IOException {
    host = s.getLocalAddress();
    port = s.getLocalPort();
    connectHost = s.getInetAddress();
    connectPort = s.getPort();

    socket = s;
    initialiseSocket();

    superPeer = newSuperPeer;
    status = DistConstants.status.DISCONNECTED;
    commandingPeer = c;
    running = true;
  }

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param nHost host name to bind to.
   * @param nPort port number to bind to.
   * @param nConnHost host name to connect to.
   * @param nConnPort port number to connect to.
   * @param newSuperPeer is this a super-peer?
   * @throws IOException 
   */
  public ClusterPeer(final NetworkPeer c, final InetAddress nHost, final int nPort,
      final InetAddress nConnHost, final int nConnPort, final boolean newSuperPeer) throws IOException {
    host = nHost;
    connectHost = nConnHost;
    port = nPort;
    connectPort = nConnPort;

    superPeer = newSuperPeer;
    status = DistConstants.status.DISCONNECTED;
    commandingPeer = c;
    running = true;

    socket = new Socket(connectHost, connectPort, host, port);
    initialiseSocket();
  }

  /**
   * Creates the communication socket to the other peer.
   * @throws IOException 
   */
  private void initialiseSocket() throws IOException {
    in = new DataInputStream(socket.getInputStream());
    out = new DataOutputStream(socket.getOutputStream());
    listener = new ClusterPeerListener(this);
    new Thread(listener).start();
  }

  /**
   * Sets if this is a super-peer or not.
   *
   * @param v is this a super-peer?
   */
  public void setSuperPeer(final boolean v) {
    superPeer = v;
  }

  /**
   * Returns true, if this node is a super-peer.
   *
   * @return true, if super-peer
   */
  public boolean isSuperPeer() {
    return superPeer;
  }

  /**
   * Changes the status of this peer and returns the former value.
   *
   * @param s new status
   * @return old status value
   */
  public DistConstants.status changeStatus(final DistConstants.status s) {
    DistConstants.status old = status;
    status = s;
    return old;
  }

  /**
   * Returns the status of this peer.
   *
   * @return Status of this peer.
   */
  public DistConstants.status getStatus() {
    return status;
  }

  /**
   * Returns a unique identifier for this peer.
   *
   * @return A unique identifier.
   */
  public String getIdentifier() {
    return host + ":" + port + ":" + connectHost + ":" + connectPort;
  }

  /**
   * Returns the host name to be used for connection attempts to
   * this peer as byte array.
   *
   * @return host name
   */
  public byte[] getConnectionHostAsByte() {
    return connectHost.getAddress();
  }

  /**
   * Returns the port number to be used for connection attempts to
   * this peer.
   * @return port number.
   */
  public int getConnectionPort() {
    return connectPort;
  }

  /**
   * Handle incoming connects from other normal peers
   * and establishes the connection.
   */
  protected void handleConnectFromNormalpeer() {
    try {
      out.write(DistConstants.P_CONNECT_NORMAL_ACK);

      byte packetIn = in.readByte();
      if (packetIn == DistConstants.P_CONNECT_NORMAL) {
        status = DistConstants.status.PENDING;
        commandingPeer.addPeerToNetwork(this);

        byte[] sendHost = commandingPeer.serverSocket.getInetAddress().getAddress();
        out.writeInt(sendHost.length);
        out.write(sendHost);
        out.writeInt(commandingPeer.serverSocket.getLocalPort());

        out.write(DistConstants.P_CONNECT_ACK);
        changeStatus(DistConstants.status.CONNECTED);
      } else if (packetIn == DistConstants.P_CONNECT) {
        // this is a normal peer, but he has to connect to the super-peer,
        // so the address of the super-peer is sent.
        out.write(DistConstants.P_SUPERPEER_ADDR);
        byte[] bHost = commandingPeer.superPeer.connectHost.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.superPeer.connectPort);
      } else {
        running = false;
      }
    } catch (IOException e) {
      commandingPeer.log.write("I/O error on the socket connection to " +
          getIdentifier());
    }
  }

  /**
   * Handle the incoming connection attempt from a super-peer.
   * As this is a normal peer, the new super-peer has to talk to an
   * already existing super-peer, so the super-peer of this peer is sent.
   */
  protected void handleConnectFromSuperpeer() {
    try {
      // this is a normal peer, but he has to connect to a super-peer,
      // so the address of the super-peer of this cluster is sent.
      out.write(DistConstants.P_SUPERPEER_ADDR);
      byte[] bHost = commandingPeer.superPeer.connectHost.getAddress();
      out.writeInt(bHost.length);
      out.write(bHost, 0, bHost.length);
      out.writeInt(commandingPeer.superPeer.connectPort);
    } catch (IOException e) {
      commandingPeer.log.write("I/O error on the socket connection to " +
          getIdentifier());
    }
  }

  /**
   * Connect to the peer on the other side of the already
   * opened socket. This is the first connect, so request information
   * about the network and other peers.
   *
   * @return success
   */
  protected boolean initiateConnect() {
    try {
      status = DistConstants.status.PENDING;
      out.write(DistConstants.P_CONNECT);
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }

    return true;
  }

  protected boolean connect() {
    try {
      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectHost = InetAddress.getByAddress(nbHost);
      connectPort = in.readInt();

      if (superPeer) {
        commandingPeer.addPeerToNetwork(this);
        out.write(DistConstants.P_CONNECT_SEND_PEERS);
        byte[] bHost = commandingPeer.host.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.port);

        int nnodes = in.readInt();
        for(int i = 0; i < nnodes; ++i) {
          int length2 = in.readInt();
          byte[] nbHost2 = new byte[length2];
          in.read(nbHost2, 0, length2);
          InetAddress cHost = InetAddress.getByAddress(nbHost2);
          int cPort = in.readInt();
          commandingPeer.connectToPeer(cHost, cPort);
        }

        out.write(DistConstants.P_CONNECT_NODES_ACK);
        changeStatus(DistConstants.status.CONNECTED);
        commandingPeer.superPeer = this;
      }

      return true;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }
  }

  protected void otherAddress() {
    try {
      int length = in.readInt();
      byte[] nHost = new byte[length];
      in.read(nHost, 0, length);

      // set a new host to connect to, which should be a super-peer
      connectHost = InetAddress.getByAddress(nHost);
      connectPort = in.readInt();
      initialiseSocket();

      initiateConnect();
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
    }
  }

  /**
   * Connect to the peer on the other side of the already
   * opened socket. Do the connection simple and do not request
   * information about the network.
   *
   * @return success
   */
  protected boolean initiateSimpleConnect() {
    try {
      status = DistConstants.status.PENDING;
      out.write(DistConstants.P_CONNECT);

      return true;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }
  }

  protected boolean connectSimple() {
    try {
      out.write(DistConstants.P_CONNECT_NORMAL);

      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectHost = InetAddress.getByAddress(nbHost);
      connectPort = in.readInt();

      if (in.readByte() == DistConstants.P_CONNECT_ACK) {
        changeStatus(DistConstants.status.CONNECTED);
        return true;
      }

      changeStatus(DistConstants.status.CONNECT_FAILED);
      return false;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }
  }

  /**
   * Sends a XQuery to the connected peer and execute it there.
   */
  protected void executeXQuery() {
    try {
      out.write(DistConstants.P_XQUERY);
      out.write(commandingPeer.xquery.length());
      out.writeBytes(commandingPeer.xquery);
    } catch(IOException e) {
      commandingPeer.log.write("Failed to execute a remote XQuery.");
    }
  }

  /**
   * Closes all open connections of this peer.
   */
  public void close() {
    try {
      status = DistConstants.status.DISCONNECTED;

      if (in != null)
        in.close();
      if (out != null)
        out.close();
      if (socket != null && socket.isBound())
        socket.close();
    } catch(IOException e) {
      commandingPeer.log.write("Could not close the connection in a clean way.");
    }
  }

  /**
   * Handles an incoming request to process an XQuery and returns
   * the serialized result to the other peer.
   */
  protected void handleXQuery() {
    try {
      int length = in.readInt();
      byte[] query = new byte[length];
      in.read(query);

      Query q = new Query(query.toString(), commandingPeer.ctx);
      q.execute(false, out, false);
    } catch (IOException e) {
      commandingPeer.log.write("Connection error.");
    }
  }

  @Override
  public void run() {
    while (running) {
      actionLock.lock();
      try {
        if (actionType == DistConstants.action.NONE)
          action.await();

        if (actionType == DistConstants.action.FIRST_CONNECT) {
          initiateConnect();
        } else if (actionType == DistConstants.action.SIMPLE_CONNECT) {
          initiateSimpleConnect();
        } else if (actionType == DistConstants.action.XQUERY) {
          executeXQuery();
        } else if (actionType == DistConstants.action.HANDLE_XQUERY) {
          handleXQuery();
        }

        actionType = DistConstants.action.NONE;
      } catch (InterruptedException e) {
        continue;
      } finally {
        actionLock.unlock();
      }
    }

    close();
   }

  /**
   * Listens for incoming request from the other peer.
   */
  private class ClusterPeerListener implements Runnable {
    /** Cluster-peer to handle the incoming requests. */
    ClusterPeer parent;

    /**
     * Default constructor.
     * @param p The connected cluster peer.
     */
    public ClusterPeerListener(final ClusterPeer p) {
      parent = p;
    }

    @Override
    public void run() {
      while (true) {
        try {
          byte packetIn = parent.in.readByte();
          if (packetIn == DistConstants.P_XQUERY) {
            parent.actionType = DistConstants.action.HANDLE_XQUERY;

            parent.actionLock.lock();
            parent.action.signalAll();
            parent.actionLock.unlock();
          } else if (packetIn == DistConstants.P_CONNECT) {
            handleConnectFromNormalpeer();
          } else if (packetIn == DistConstants.P_CONNECT_SUPER) {
            handleConnectFromSuperpeer();
          } else if (packetIn == DistConstants.P_CONNECT_ACK) {
            connectionLock.lock();
            connect();
            connected.signalAll();
            connectionLock.unlock();
          } else if (packetIn == DistConstants.P_CONNECT_NORMAL_ACK) {
            connectionLock.lock();
            connectSimple();
            connected.signalAll();
            connectionLock.unlock();
          } else if(packetIn == DistConstants.P_SUPERPEER_ADDR) {
            otherAddress();
          }
        } catch(IOException e) {
          parent.commandingPeer.log.write("Lost connection to other peer.");
        }
      }
    }
  }
}