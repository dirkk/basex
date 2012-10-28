package org.basex.dist;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;

import org.basex.dist.handler.*;
import org.basex.util.*;

/**
 * Another node within the same cluster as this node.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 *
 */
public class ClusterPeer {
  /** Commanding network-peer. */
  protected NetworkPeer commandingPeer;
  /** Current state of this node. */
  protected DistConstants.state state;
  /** Is this peer still active? */
  protected boolean isRunning;
  /** Host name and port of this remote peer. */
  protected InetSocketAddress remoteAddress;
  /** Socket channel for this connection. */
  protected SocketChannel channel;
  /** r/w handler for networking I/O. */
  protected ReadWriteHandler rwHandler;

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param address Host name and port of the peer connected to.
   */
  public ClusterPeer(final NetworkPeer c, final InetSocketAddress address) {
    this.remoteAddress = address;
    state = DistConstants.state.DISCONNECTED;
    commandingPeer = c;
    isRunning = true;
  }
  
  /**
   * Cluster peer constructor for an already opened socket.
   * 
   * @param c The commanding peer for this peer in the cluster.
   * @param ch channel socket, which is already opened.
   */
  public ClusterPeer(final NetworkPeer c, final SocketChannel ch) {
    try {
      this.remoteAddress = (InetSocketAddress) ch.getRemoteAddress();
    } catch(IOException e) {
      // should normally not happen
      e.printStackTrace();
    }
    state = DistConstants.state.PENDING;
    commandingPeer = c;
    isRunning = true;
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
    InetSocketAddress localAddress = commandingPeer.reactor.getSocketAddress();
    return localAddress.getHostString() + ":" + localAddress.getPort() + ":"
        + remoteAddress.getHostString() + ":" + remoteAddress.getPort();
  }

  /**
   * Returns the host name to be used for connection attempts to
   * this peer as byte array.
   *
   * @return host name
   */
  public byte[] getConnectionHostAsByte() {
    return remoteAddress.getAddress().getAddress();
  }

  /**
   * Returns the port number to be used for connection attempts to
   * this peer.
   * @return port number.
   */
  public int getConnectionPort() {
    return remoteAddress.getPort();
  }

  /**
   * Connection establishment between this peer and a super-peer. The super-peer
   * will send a list of all the peers within the cluster and this will be used
   * to connect to these normal peers.
   * The state of this ClusterPeer will be changed accordingly to the success or
   * failure of the connection establishment.
   *
   * @throws IOException socket channel could not be opened.
   */
  public void connect() throws IOException {
    /* Open and connect to remote address. */
    channel = SocketChannel.open();
    channel.configureBlocking(false);
    channel.connect(remoteAddress);
    System.out.println("Connect to " + remoteAddress.getHostName() + ":"
        + remoteAddress.getPort());
    
    /* register this channel so that this object receives CONNECT events. */
    channel.register(commandingPeer.getReactor().getNextSelector(),
        SelectionKey.OP_CONNECT, this);
  }
  
  /**
   * The selector received an CONNECT I/O event and this must be the
   * return statement from a previous connection attempt for this peer. So
   * handle the connection establishment and also send some initial
   * information.
   */
  public void handleConnect() {
    try {
      if (!channel.finishConnect()) {
        connectionFailed(null);
      }
      
      // set socket buffer sizes
      // TODO set some reasonable values, get an idea how to determine this...
      channel.socket().setReceiveBufferSize(2048);
      channel.socket().setSendBufferSize(2048);
      
      rwHandler = new ReadWriteHandler(channel,
          commandingPeer.getReactor().getNextSelector() , this);

      // connection was successfully established
      state = DistConstants.state.CONNECTED;
    } catch(IOException e) {
      connectionFailed(e);
    }
  }
  
  /**
   * A connection attempt to the remote peer failed. This peer and thread will
   * therefore be gracefully be shutdown.
   * @param ex The cause of the connection failure
   */
  private void connectionFailed(Exception ex) {
    state = DistConstants.state.CONNECT_FAILED;
    if (ex == null) {
      Util.errln("A connection attempt to the cluster peer " + getIdentifier() +
        "failed.");
    } else {
      Util.errln("A connection attempt to the cluster peer " + getIdentifier() +
          "failed with the exception:");
      Util.errln("Exception", ex);
    }
    close();
  }
  
  /**
   * A new fully reassembled packet arrived for this peer. The function
   * processes the packet and reacts according to the protocol.
   *
   * @param packet The newly arrived packet.
   */
  public void packetArrived(ByteBuffer packet) {
    // TODO everything
  }

  /**
   *  Disconnects the connection.
   */
  public void disconnect() {
    // TODO: write connected peers that this peer is going offline
  }

  /**
   * Handles a disconnect message from the connected peer.
   */
  protected void handleDisconnect() {
    // TODO close listening socket
    commandingPeer.peers.remove(getIdentifier());
  }

  /**
   * Closes all open connections of this peer.
   */
  public void close() {
    try {
      state = DistConstants.state.DISCONNECTED;
      isRunning = false;

      rwHandler.close();
      channel.close();
    } catch(IOException e) {
      Util.errln("Could not gracefully close the peer: ", e);
    }
  }

}