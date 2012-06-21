package org.basex.dist;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;
import org.basex.util.*;

import static org.basex.core.Text.*;

/**
 * Another node within the same cluster as this node.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 *
 */
public class ClusterPeer implements Runnable {
  /** Does this represent a connection to a super-peer? */
  protected boolean superPeer;
  /** Commanding network-peer. */
  protected NetworkPeer commandingPeer;
  /** Current state of this node. */
  protected DistConstants.state state;
  /** Connection socket. */
  protected Socket socket;
  /** Data input stream. */
  protected DataInputStream in;
  /** Data output stream. */
  protected DataOutputStream out;
  /** Is this peer still active? */
  protected boolean running;
  /** Host name. */
  protected final InetAddress host;
  /** Port number. */
  protected final int port;
  /** Host this peer should connect to. */
  protected InetAddress connectHost;
  /** Port number this peer should connect to. */
  protected int connectPort;
  /** Action lock. */
  public final Lock actionLock = new ReentrantLock();
  /** To notify of new events. */
  public Condition action = actionLock.newCondition();
  /** Connection lock. */
  public final Lock connectionLock = new ReentrantLock();
  /** Connect condition. */
  public Condition connected = connectionLock.newCondition();
  /** Execute this action next. */
  public DistConstants.action actionType;
  /** Listener for incoming requests from the other endpoint. */
  protected ClusterPeerListener listener;
  /** Queue of XQueries to be executed. */
  public Map<Integer, DistributedQuerySingle> queueXQueries;

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The socket to communicate to.
   */
  public ClusterPeer(final NetworkPeer c, final Socket s) {
    host = s.getLocalAddress();
    port = s.getLocalPort();
    connectHost = s.getInetAddress();
    connectPort = s.getPort();

    socket = s;
    initialiseSocket();

    superPeer = false;
    state = DistConstants.state.DISCONNECTED;
    commandingPeer = c;
    running = true;

    queueXQueries = new LinkedHashMap<Integer, DistributedQuerySingle>();
  }

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param nHost host name to bind to.
   * @param nPort port number to bind to.
   * @param nConnHost host name to connect to.
   * @param nConnPort port number to connect to.
   * @param newSuperPeer is this a connection to a super-peer?
   */
  public ClusterPeer(final NetworkPeer c, final InetAddress nHost, final int nPort,
      final InetAddress nConnHost, final int nConnPort, final boolean newSuperPeer) {
    host = nHost;
    connectHost = nConnHost;
    port = nPort;
    connectPort = nConnPort;

    superPeer = newSuperPeer;
    state = DistConstants.state.DISCONNECTED;
    commandingPeer = c;
    running = true;

    try {
      socket = new Socket(connectHost, connectPort, host, port);
    } catch(BindException e) {
      Util.outln(D_BIND_ERROR_X, host.toString() + port);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
    initialiseSocket();

    queueXQueries = new LinkedHashMap<Integer, DistributedQuerySingle>();
  }

  /**
   * Creates the communication socket to the other peer.
   */
  private void initialiseSocket() {
    try {
      in = new DataInputStream(socket.getInputStream());
      out = new DataOutputStream(socket.getOutputStream());
      listener = new ClusterPeerListener();
      new Thread(listener).start();
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
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
   * Returns true if this a connection to a super-peer.
   *
   * @return true if super-peer
   */
  public boolean isSuperPeer() {
    return superPeer;
  }

  /**
   * Changes the status of this peer and returns the old value.
   *
   * @param s new status
   * @return old status value
   */
  public DistConstants.state changeStatus(final DistConstants.state s) {
    DistConstants.state old = state;
    state = s;
    return old;
  }

  /**
   * Returns the current status of this peer.
   *
   * @return Status of this peer.
   */
  public DistConstants.state getStatus() {
    return state;
  }

  /**
   * Returns a unique identifier for this connection.
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
        /* This peer really wants to connect to us */
        state = DistConstants.state.PENDING;
        commandingPeer.addPeerToNetwork(this);

        /* Send the host and port to connect to this port  for new connections */
        byte[] sendHost = commandingPeer.serverSocket.getInetAddress().getAddress();
        out.writeInt(sendHost.length);
        out.write(sendHost);
        out.writeInt(commandingPeer.serverSocket.getLocalPort());

        out.write(DistConstants.P_CONNECT_ACK);
        changeStatus(DistConstants.state.CONNECTED);
      } else if (packetIn == DistConstants.P_CONNECT) {
        /* this is a normal peer, but he has to connect to the super-peer,
         * so the address of the super-peer is sent. */
        sendAddressSuperPeer();
        changeStatus(DistConstants.state.DISCONNECTED);
      } else {
        changeStatus(DistConstants.state.CONNECT_FAILED);
      }
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  /**
   * Sends the address of the super-peer of this normal peer to the
   * connected other peer.
   */
  protected void sendAddressSuperPeer() {
    try {
      out.write(DistConstants.P_SUPERPEER_ADDR);
      byte[] bHost = commandingPeer.superPeer.connectHost.getAddress();
      out.writeInt(bHost.length);
      out.write(bHost);
      out.writeInt(commandingPeer.superPeer.connectPort);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
  }

  /**
   * Handle the incoming connection attempt from a super-peer.
   * As this is a normal peer, the new super-peer has to talk to an
   * already existing super-peer, so the super-peer of this peer is sent.
   */
  protected void handleConnectFromSuperpeer() {
    sendAddressSuperPeer();
  }

  /**
   * Connect to the peer on the other side of the already
   * opened socket. This is the first connect, so request information
   * about the network and other peers. This just initiates the connection,
   * the response is handled by the {@link ClusterPeerListener} class.
   */
  protected void initiateConnect()  {
    try {
      state = DistConstants.state.PENDING;
      out.write(DistConstants.P_CONNECT);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  /**
   * Connection establishment between this peer and a super-peer. The super-peer
   * will send a list of all the peers within the cluster and this will be used
   * to connect to these normal peers.
   * The state of this ClusterPeer will be changed accordingly to the success or
   * failure of the connection establishment.
   */
  protected void connect() {
    try {
      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost);
      connectHost = InetAddress.getByAddress(nbHost);
      connectPort = in.readInt();

      if (superPeer) {
        /* The first connection has to be done to a super-peer */
        commandingPeer.addPeerToNetwork(this);

        out.write(DistConstants.P_CONNECT_SEND_PEERS);

        /* Send the public host and port for new incoming connections to the
         * other peer. */
        byte[] bHost = commandingPeer.host.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost);
        out.writeInt(commandingPeer.port);

        /* Reads a list of all the other normal peers within this cluster and
         * establishes a connection to them. */
        int nnodes = in.readInt();
        for(int i = 0; i < nnodes; ++i) {
          int length2 = in.readInt();
          byte[] nbHost2 = new byte[length2];
          in.read(nbHost2);
          InetAddress cHost = InetAddress.getByAddress(nbHost2);
          int cPort = in.readInt();
          if (!commandingPeer.connectToPeer(cHost, cPort)) {
            changeStatus(DistConstants.state.DISCONNECTED);
            return;
          }
        }

        out.write(DistConstants.P_CONNECT_NODES_ACK);
        changeStatus(DistConstants.state.CONNECTED);
        commandingPeer.superPeer = this;
      } else {
        changeStatus(DistConstants.state.DISCONNECTED);
      }
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
  }

  /**
   * A normal peer tries to connect to this cluster, but the first connection
   * has to be to the super-peer. As this is a normal peer, the peer now send the
   * address of the actual super-peer of this cluster.
   */
  protected void handleNewSuperPeerAddress() {
    try {
      int length = in.readInt();
      byte[] nHost = new byte[length];
      in.read(nHost);

      // set a new host to connect to, which should be a super-peer
      connectHost = InetAddress.getByAddress(nHost);
      connectPort = in.readInt();
      initialiseSocket();

      initiateConnect();
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.DISCONNECTED);
    }
  }

  /**
   * Connect to the peer on the other side of the already
   * opened socket. Do the connection simple and do not request
   * information about the network.
   */
  protected void initiateSimpleConnect() {
    try {
      state = DistConstants.state.PENDING;
      out.write(DistConstants.P_CONNECT);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  /**
   * Connects this peer with another normal peer.
   */
  protected void connectSimple() {
    try {
      state = DistConstants.state.PENDING;
      commandingPeer.addPeerToNetwork(this);
      out.write(DistConstants.P_CONNECT_NORMAL);

      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectHost = InetAddress.getByAddress(nbHost);
      connectPort = in.readInt();

      if (in.readByte() == DistConstants.P_CONNECT_ACK) {
        changeStatus(DistConstants.state.CONNECTED);
      } else {
        changeStatus(DistConstants.state.CONNECT_FAILED);
      }
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  /**
   *  Disconnects the connection.
   */
  public void disconnect() {
    try {
      out.write(DistConstants.P_DISCONNECT);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
  }

  /**
   * Handles a disconnect message from the connected peer.
   */
  protected void handleDisconnect() {
    // TODO close listening socket
    commandingPeer.peers.remove(getIdentifier());
  }

  /**
   * Sends a XQuery to the connected peer and executes it there.
   */
  protected synchronized void executeXQuery() {
    try {
      Iterator<DistributedQuerySingle> it = queueXQueries.values().iterator();
      while (it.hasNext()) {
        DistributedQuerySingle q = it.next();
        out.write(DistConstants.P_XQUERY);
        out.writeInt(q.seq);
        out.writeInt(q.query.length());
        out.write(q.query.getBytes());
        q.state = DistConstants.queryState.SEND;
      }
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.DISCONNECTED);
    }
  }

  /**
   * Closes all open connections of this peer.
   */
  public void close() {
    try {
      state = DistConstants.state.DISCONNECTED;
      running = false;

      if (in != null)
        in.close();
      if (out != null)
        out.close();
      if (socket != null && socket.isBound())
        socket.close();
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
  }

  /**
   * Handles an incoming request to process an XQuery and returns
   * the serialized result to the other peer.
   */
  protected void handleXQuery() {
    try {
      int inSeq = in.readInt();
      int length = in.readInt();
      byte[] query = new byte[length];
      in.read(query);

      out.write(DistConstants.P_RESULT_XQUERY);
      out.writeInt(inSeq);
      Query q = new Query(new String(query), commandingPeer.ctx);
      try {
        q.execute(false, out, false);
      } catch(IOException e) {
        Util.outln(e.getLocalizedMessage());
      }
      out.writeByte(0);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.DISCONNECTED);
    }
  }

  /**
   * An XQuery has been processed and evaluated on the other peer.
   * This peer now sends the serialized result and this function processes
   * this.
   */
  protected void handleResultXQuery() {
    try {
      int seq = in.readInt();
      DistributedQuerySingle query = queueXQueries.get(seq);
      String r = new String();
      while (true) {
        byte packetIn = in.readByte();
        if (packetIn == 0)
          break;

        r += String.valueOf((char) packetIn);
      }
      query.result = r;
      commandingPeer.processXQueryResult(query);
      queueXQueries.remove(seq);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.DISCONNECTED);
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
    /**
     * Default constructor.
     */
    public ClusterPeerListener() {
    }

    @Override
    public void run() {
      while (running) {
        byte packetIn = 0;
        try {
          packetIn = in.readByte();

        } catch(IOException e) {
          commandingPeer.log.write("Lost connection to other peer.");
        }
        if (packetIn == DistConstants.P_XQUERY) {
          handleXQuery();
        } else if (packetIn == DistConstants.P_RESULT_XQUERY) {
          handleResultXQuery();
        } else if (packetIn == DistConstants.P_CONNECT) {
          handleConnectFromNormalpeer();
          if (getStatus() != DistConstants.state.CONNECTED)
            running = false;
        } else if (packetIn == DistConstants.P_CONNECT_SUPER) {
          handleConnectFromSuperpeer();
          if (getStatus() != DistConstants.state.CONNECTED)
            running = false;
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
          handleNewSuperPeerAddress();
        } else if(packetIn == DistConstants.P_DISCONNECT) {
          handleDisconnect();
        }
      }
    }
  }
}