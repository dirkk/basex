package org.basex.dist;

import static org.basex.core.Text.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.*;

import org.basex.core.*;
import org.basex.server.*;
import org.basex.util.*;

/**
 * Class to join a distributed BaseX network as a normal peer. This peer is connected to
 * exactly one super-peer and to a not specified number of other normal-peers. It allows
 * to distribute the storing and execution of functionality of BaseX.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class NetworkPeer implements Runnable {
  /** Server socket to listen for new incoming connections. */
  protected ServerSocket serverSocket;
  /** Is this peer running? */
  public volatile boolean running;
  /** List of connected peers, also including the super-peer. */
  protected Map<String, ClusterPeer> peers;
  /** Log file. */
  protected Log log;
  /** Host name for listening socket. */
  protected final InetAddress host;
  /** Port number for listening socket. */
  protected final int port;
  /** Reference to the database context. */
  protected final Context ctx;
  /** Reference to the super-peer, null if super-peer itself. */
  protected ClusterPeer superPeer;
  /** Lock to be hold during connection establishment. */
  public Lock connectionLock;
  /** Wait to be connected. */
  public Condition connected;
  /** Maximum sequence number for any XQuery. */
  private Integer maxSeq;

  /**
   * Makes this instance of BaseX to a peer in a distributed BaseX network.
   *
   * @param nHost host to listen to
   * @param nPort port number to listen to
   * @param context The database context
   * @throws UnknownHostException unknown host name.
   */
  public NetworkPeer(final String nHost, final int nPort, final Context context)
      throws UnknownHostException {
    ctx = context;
    log = new Log(ctx, false);
    connectionLock = new ReentrantLock();
    connected = connectionLock.newCondition();
    port = nPort;
    host = InetAddress.getByName(nHost);

    maxSeq = 0;

    try {
      serverSocket = new ServerSocket();
      serverSocket.setReuseAddress(true);
      /* use a socket timeout to check regularly if the connection was quit */
      serverSocket.setSoTimeout(5000);
      serverSocket.bind(new InetSocketAddress(host, port));
      running = true;
    } catch(BindException e) {
      Util.outln(D_BIND_ERROR_X, host.toString() + port);
    } catch(IOException e) {
      Util.outln(D_SOCKET_WAIT_ERROR_X, serverSocket.getInetAddress().toString()
          + serverSocket.getLocalPort());
    }

    peers = new LinkedHashMap<String, ClusterPeer>();
  }

  /**
   * Adds a new peer to the internal  connection table of
   * this peer.
   * @param cp The peer to add
   */
  protected void addPeerToNetwork(final ClusterPeer cp) {
    peers.put(cp.getIdentifier(), cp);
  }

  /**
   * Connects this peer to another normal peer.
   *
   * @param cHost The host name to connect to
   * @param cPort The port number to connect to
   * @return success.
   */
  public boolean connectToPeer(final InetAddress cHost, final int cPort) {
    ClusterPeer newPeer = new ClusterPeer(this, host, getNextFreePort(), cHost,
        cPort, false);
    newPeer.actionType = DistConstants.action.SIMPLE_CONNECT;
    return connectClusterPeer(newPeer);
  }

  /**
   * Starts a new Thread based on a given peer within the cluster
   * and connect to this peer.
   *
   * @param cp The peer to connect to
   * @return connection establishment successful
   */
  protected boolean connectClusterPeer(final ClusterPeer cp) {
    new Thread(cp).start();
    cp.actionLock.lock();
    cp.action.signalAll();
    cp.actionLock.unlock();

    cp.connectionLock.lock();
    try {
      cp.connected.await();
    } catch(InterruptedException e) {
      log.write("Interrupt exception");
      return false;
    } finally {
      cp.connectionLock.unlock();
    }

    if (cp.getStatus() == DistConstants.state.CONNECTED)
      return true;

    return false;
  }

  /**
   * Returns the next free port, starting from the initial port of
   * this peer.
   *
   * @return next free port.
   */
  protected int getNextFreePort() {
    return port + peers.values().size() + 1;
  }

  /**
   * Join an already established cluster of a BaseX network. You have to connect
   * to the super-peer of this cluster. If not a normal peer will send the address
   * of the super-peer and the connection establishment is done again.
   *
   * @param cHost the name of the node to connect to.
   * @param cPort the port number of the node to connect to.
   * @return success
   */
  public boolean connectToCluster(final InetAddress cHost, final int cPort) {
    ClusterPeer newPeer = new ClusterPeer(this, host, port +
          peers.values().size() + 1, cHost, cPort, true);

    newPeer.actionType = DistConstants.action.FIRST_CONNECT;

    return connectClusterPeer(newPeer);
  }

  /**
   * Shuts down this peer and frees all resources.
   *
   */
  private void close() {
    /* Sends a message to each connected peer to notify of disconnect. */
    for (ClusterPeer cp : peers.values())
      cp.disconnect();

    /* Free resources. */
    try {
      if (serverSocket != null && serverSocket.isBound()) {
        serverSocket.close();
      }
    } catch (IOException e) {
      Util.outln(D_SOCKET_FREE_FAILED_X, serverSocket.getInetAddress().toString()
          + serverSocket.getLocalPort());
    }
  }

  /**
   * Stops this peer, disconnects from the network and discontinue the
   * distributed mode.
   */
  public void stop() {
    running = false;
  }

  /**
   * Is this a super-peer itself?
   * @return boolean true, if super-peer.
   */
  public boolean isSuperPeer() {
    if(superPeer == null)
      return true;

    return false;
  }

  /**
   * Gives information about this peer and connected peers.
   * @return information about this peer.
   */
  public String info() {
    String o = new String();
    if (superPeer != null) {
      o += "Super-Peer: " + superPeer.socket.getInetAddress().toString() + ":" +
          superPeer.socket.getPort() + "\r\n";
    } else {
      o += "Super-Peer: No super-peer registered.\r\n";
    }

    o += "Normal peer: " + getIdentifier() + "\r\n";
    for(ClusterPeer c : peers.values()) {
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

  public void outputXQueryResult(String result) {
    //TODO
  }

  @Override
  public void run() {
    while (running) {
      try {
        /* Waiting for another node to connect */
        try {
          Socket socketIn = serverSocket.accept();
          socketIn.setReuseAddress(true);

          ClusterPeer cn = new ClusterPeer(this, socketIn);
          Thread t = new Thread(cn);
          t.start();
        } catch(SocketTimeoutException e) {
          continue;
        }
      } catch(IOException e) {
        Util.outln(D_SOCKET_WAIT_ERROR_X, serverSocket.getInetAddress().toString()
            + serverSocket.getLocalPort());
        stop();
      }
    }

    close();
  }
}