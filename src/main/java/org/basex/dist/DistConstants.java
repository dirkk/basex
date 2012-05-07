package org.basex.dist;

/**
 * Constants and magic values used by the P2P network and distributed version of BaseX.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 *
 */
public final class DistConstants {
  /** Packet message: Connect to a cluster. */
  public static final byte P_CONNECT = 0x01;
  /** Packet message: Connect as a new super-peer to the network. */
  public static final byte P_CONNECT_SUPER = 0x08;
  /**
   * Packet message: Connection attempt successful as super-peer, connect to the following
   * super-peer now.
   */
  public static final byte P_AVAILABLE_SUPERPEER = 0x09;
  /** Packet message: Connect successful. */
  public static final byte P_CONNECT_ACK = 0x02;
  /** packet message: Connect failed. */
  public static final byte P_CONNECT_FAILED = 0x03;
  /** Packet message: Send all cluster nodes to the new node. */
  public static final byte P_CONNECT_NODES = 0x04;
  /** Packet message: Got all cluster nodes successfully. */
  public static final byte P_CONNECT_NODES_ACK = 0x05;
  /** Packet message: A new node has been added to the cluster. */
  public static final byte P_NEW_NODE = 0x06;
  /** Packet message: Node successfully added. */
  public static final byte P_NEW_NODE_ACK = 0x07;
  /** Packet message: I am a super-peer. */
  public static final byte P_SUPERPEER_ME = 0x0A;
  /** Packet message: The following peer is a super-peer. */
  public static final byte P_SUPERPEER_ADDR = 0x0B;
  /** Packet message: Are you a super-peer, if not, who is your super-peer? */
  public static final byte P_SUPERPEER_NEAREST = 0x0C;
  /** Packet message: add me to your cluster. */
  public static final byte P_SUPERPEER_ADD_ME = 0x0D;

  /** connection status of this network node within the cluster */
  public enum status {
    DISCONNECTED, PENDING, CONNECTED
  };
}
