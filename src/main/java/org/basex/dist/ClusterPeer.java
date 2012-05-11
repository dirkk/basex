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
  /** lock. */
  public final Lock lock = new ReentrantLock();
  /** to notify of new events. */
  public Condition action = lock.newCondition();
  /** connect to a cluster. This is the first attempt, so request some information. */
  public boolean doFirstConnect;
  /** connect to a peer, do not request information about the network. */
  public boolean doSimpleConnect;
  /** handle an incoming connection. */
  public boolean doHandleConnect;
  /** The host name for new connection attempts for this peer. */
  protected InetAddress connectionHost;
  /** The port number for new connection attempts for this peer. */
  protected int connectionPort;

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The connection to talk to this peer.
   */
  public ClusterPeer(final NetworkPeer c, final Socket s) {
    this(c, s, false);
  }

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The connection to talk to this peer.
   * @param newSuperPeer Reference to the super-peer. If no super-peer, null.
   */
  public ClusterPeer(final NetworkPeer c, final Socket s,
      final boolean newSuperPeer) {
    socket = s;
    superPeer = newSuperPeer;
    status = DistConstants.status.PENDING;
    commandingPeer = c;
    running = true;

    try {
      in = new DataInputStream(socket.getInputStream());
      out = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      commandingPeer.log.write("Could not create I/O on the socket.");
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
    return socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
  }

  /**
   * Returns the host name to be used for connection attempts to
   * this peer as byte array.
   *
   * @return host name
   */
  public byte[] getConnectionHostAsByte() {
    return connectionHost.getAddress();
  }

  /**
   * Returns the port number to be used for connection attempts to
   * this peer.
   * @return port number.
   */
  public int getConnectionPort() {
    return connectionPort;
  }

  /**
   * Handle incoming connects from other normal peers
   * and establishes the connection.
   */
  protected void handleConnectFromNormalpeer() {
    try {
      byte packetIn = in.readByte();
      if(packetIn == DistConstants.P_CONNECT_NORMAL) {
        commandingPeer.addPeerToNetwork(this);

        out.write(DistConstants.P_CONNECT_NORMAL_ACK);
      } else if (packetIn == DistConstants.P_CONNECT) {
        // this is a normal peer, but he has to connect to the super-peer,
        // so the address of the super-peer is sent.
        out.write(DistConstants.P_SUPERPEER_ADDR);
        byte[] bHost = commandingPeer.superPeer.connectionHost.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.superPeer.connectionPort);
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
      byte[] bHost = commandingPeer.superPeer.connectionHost.getAddress();
      out.writeInt(bHost.length);
      out.write(bHost, 0, bHost.length);
      out.writeInt(commandingPeer.superPeer.connectionPort);
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
  protected boolean connect() {
    try {
      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectionHost = InetAddress.getByAddress(nbHost);
      connectionPort = in.readInt();

      if (superPeer) {
        out.write(DistConstants.P_CONNECT_SEND_SUPERPEERS);
        byte[] bHost = commandingPeer.host.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.port);

        int nnodes = in.readInt();
        for (int i = 0; i < nnodes; ++i) {
          int length2 = in.readInt();
          byte[] nbHost2 = new byte[length2];
          in.read(nbHost2, 0, length2);
          InetAddress cHost = InetAddress.getByAddress(nbHost2);
          int cPort = in.readInt();
          commandingPeer.connectToPeer(cHost, cPort);
        }

        out.write(DistConstants.P_CONNECT_NODES_ACK);
        commandingPeer.addPeerToNetwork(this);
      } else {
        // not a super-peer
        out.write(DistConstants.P_CONNECT_NORMAL);
        if (in.readByte() == DistConstants.P_CONNECT_NORMAL_ACK) {
          status = DistConstants.status.CONNECTED;
          return true;
        }
        return false;
      }

      return true;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }
  }

  /**
   * Connect to the peer on the other side of the already
   * opened socket. Do the connection simple and do not request
   * information about the network.
   *
   * @return success
   */
  protected boolean connectSimple() {
    try {
      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectionHost = InetAddress.getByAddress(nbHost);
      connectionPort = in.readInt();

      out.write(DistConstants.P_CONNECT_NORMAL);
      if (in.readByte() == DistConstants.P_CONNECT_NORMAL_ACK) {
        status = DistConstants.status.CONNECTED;
        return true;
      }
      return false;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
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
   * Handle incoming connects from other peers
   * and establishes the connection.
   */
  protected void handleIncomingConnect() {
    try {
      byte packetIn = in.readByte();
      if(packetIn == DistConstants.P_CONNECT) {
        handleConnectFromNormalpeer();
      } else if (packetIn == DistConstants.P_CONNECT_SUPER) {
        handleConnectFromSuperpeer();
      }
    } catch (IOException e) {
      commandingPeer.log.write("I/O error on the socket connection to " +
          getIdentifier());
    }
  }

  @Override
  public void run() {
    while (running) {
      lock.lock();
      try {
        if (!doHandleConnect && !doFirstConnect && !doSimpleConnect)
          action.await();

        if (doHandleConnect) {
          doHandleConnect = false;
          handleIncomingConnect();
        }
        if (doFirstConnect) {
          doFirstConnect = false;
          connect();
        }
        if (doSimpleConnect) {
          doSimpleConnect = false;
          connectSimple();
        }
      } catch (InterruptedException e) {
        continue;
      } finally {
        lock.unlock();
      }
    }

    close();
  }
}