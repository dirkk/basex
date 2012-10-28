package org.basex.dist;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.*;

import org.basex.core.*;
import org.basex.dist.work.*;
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
  /** Is this peer running? */
  protected boolean isRunning;
  /** List of connected peers, also including the super-peer. */
  protected Map<String, ClusterPeer> peers;
  /** Reference to the database context. */
  protected final Context ctx;
  /** Reactor to handle network I/O. */
  protected Reactor reactor;
  /** Work queue for actions this peer should do. */
  protected LinkedList<WorkPacket> workQueue;

  /**
   * Makes this instance of BaseX to a peer in a distributed BaseX network.
   *
   * @param host host to listen to
   * @param port port number to listen to
   * @param context The database context
   * @throws IOException Server listening socket could not be bound
   */
  public NetworkPeer(final String host, final int port, final Context context)
      throws IOException {
    ctx = context;
    
    reactor = new Reactor(host, port, this);
    if (reactor != null) {
      isRunning = true;
    } else {
      isRunning = false;
    }

    peers = new LinkedHashMap<String, ClusterPeer>();
    workQueue = new LinkedList<WorkPacket>();
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
   * Returns a peer within the cluster with the given ID.
   * 
   * @param identifier ID of the wanted peer.
   * @return cluster peer
   */
  public ClusterPeer getPeer(final String identifier) {
    return peers.get(identifier);
  }

  /**
   * Join an already established cluster of a BaseX network. The method will
   * either successfully establish a connection or the connection attempt
   * fails. This method is non-blocking.
   *
   * @param remoteAddress remote host name and port
   */
  public void connect(final InetSocketAddress remoteAddress) {
    ClusterPeer cp = new ClusterPeer(this, remoteAddress);
    addAction(new Connect(cp));
  }
  
  /**
   * A incoming connection was established by the acceptor. The following
   * connection establishment should be handled by a new cluster peer.
   * @param ch New socket
   */
  public void connectionEstablished(SocketChannel ch) {
    try {
      ClusterPeer cp = new ClusterPeer(this,
          (InetSocketAddress) ch.getRemoteAddress());
      addAction(new ConnectionAccept(cp));
    } catch(IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Returns the reactor.
   * 
   * @return active reactor.
   */
  public Reactor getReactor() {
    return reactor;
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
      reactor.close();
    } catch (IOException e) {
      Util.outln(e.getStackTrace());
    }
  }

  /**
   * Stops this peer, disconnects from the network and discontinue the
   * distributed mode.
   */
  public void stop() {
    isRunning = false;
  }

  /**
   * Gives information about this peer and connected peers.
   * @return information about this peer.
   */
  public String info() {
    String o = new String();

    o += "Id: " + getIdentifier() + "\r\n";
    for(ClusterPeer c : peers.values()) {
      o += "|--- " + c.getIdentifier() + "\r\n";
    }

    return o;
  }
  
  /**
   * Adds a new action to the work queue to be executed.
   *
   * @param wp packet to be executed
   */
  public void addAction(WorkPacket wp) {
    workQueue.push(wp);
  }

  /**
   * Returns a unique identifier for this peer.
   *
   * @return A unique identifier.
   */
  public String getIdentifier() {
    InetSocketAddress address = reactor.getSocketAddress();
    return address.getHostString() + ":" + address.getPort();
  }

  @Override
  public void run() {
    while (isRunning) {
      while (!workQueue.isEmpty()) {
        try {
          WorkPacket wp = workQueue.pop();
          System.out.println("Packet from " +
              reactor.getSocketAddress().getPort());
          wp.execute();
        } catch(IOException e) {
          Util.errln("I/O error during work for the network peer: ", e);
        }
      }
    }

    close();
  }
}