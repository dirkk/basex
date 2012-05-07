package org.basex.dist;

import java.net.*;

/**
 * Another node within the same cluster as this node.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 *
 */
public class ClusterPeer {
  /** Host name of the node. */
  public final InetAddress host;
  /** Port number of the node. */
  public final int port;
  /** Is this peer a super-peer? */
  private boolean superPeer;
  /** status of this node. */
  private DistConstants.status status;

  /**
   * Default constructor.
   * @param newHost Host name of the node.
   * @param newPort Port number of the node.
   * @throws UnknownHostException host is unknown.
   */
  public ClusterPeer(final String newHost, final int newPort)
      throws UnknownHostException {
    this(InetAddress.getByName(newHost), newPort, false);
  }

  /**
   * Default constructor.
   * @param newHost Host name of the node.
   * @param newPort Port number of the node.
   */
  public ClusterPeer(final InetAddress newHost, final int newPort) {
    this(newHost, newPort, false);
  }

  /**
   * Default constructor.
   * @param newHost Host name of the node.
   * @param newPort Port number of the node.
   * @param newSuperPeer true, if this peer is the super-peer of this cluster
   */
  public ClusterPeer(final InetAddress newHost, final int newPort,
      final boolean newSuperPeer) {
    host = newHost;
    port = newPort;
    superPeer = newSuperPeer;
    status = DistConstants.status.DISCONNECTED;
  }

  /**
   * Default constructor.
   * @param newHost Host name of the node.
   * @param newPort Port number of the node.
   * @param newSuperPeer true, if this peer is the super-peer of this cluster
   * @throws UnknownHostException host is unknown.
   */
  public ClusterPeer(final String newHost, final int newPort,
      final boolean newSuperPeer) throws UnknownHostException {
    this(InetAddress.getByName(newHost), newPort, newSuperPeer);
  }

  /**
   * Sets whether this is a super-peer or not.
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
    return host.getHostAddress() + ":" + port;
  }

  /**
   * Return the host name as byte array.
   *
   * @return host name
   */
  public byte[] getHostAsByte() {
    return host.getAddress();
  }
}
