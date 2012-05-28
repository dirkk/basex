package org.basex.dist;

/**
 * Constants and magic values used by the P2P network and distributed version of BaseX.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 *
 */
public final class DistConstants {
  /** Packet message: Connect to a cluster. Should just be sent to a super-peer. */
  public static final byte P_CONNECT = 0x01;
  /** Packet message: Connect as a new super-peer to the network. */
  public static final byte P_CONNECT_SUPER = 0x02;
  /** Packet message: Connect a normal peer to another normal peer. */
  public static final byte P_CONNECT_NORMAL = 0x03;
  /** Packet message: Connect normal-to-normal was successful. */
  public static final byte P_CONNECT_NORMAL_ACK = 0x04;
  /** Packet message: Connect successful. */
  public static final byte P_CONNECT_ACK = 0x05;
  /** Packet message: Got all cluster nodes successfully. */
  public static final byte P_CONNECT_NODES_ACK = 0x06;
  /** Packet message: The following peer is a super-peer. */
  public static final byte P_SUPERPEER_ADDR = 0x07;
  /** Packet message: this is my host + port for connection attempts. */
  public static final byte P_CONNECTION_ATTEMPTS = 0x08;
  /** Packet message: From super-peer to super-peer and requests to send all connected
   * super-peers. */
  public static final byte P_CONNECT_SEND_PEERS = 0x09;
  /** Packet message: execute XQuery. */
  public static final byte P_XQUERY = 0x0A;
  /** Packet message: disconnect this connection. */
  public static final byte P_DISCONNECT = 0x0B;
  /** Packet message: result of a XQuery. */
  public static final byte P_RESULT_XQUERY = 0x0C;

  /** connection status of this network node within the cluster */
  public enum status {
    DISCONNECTED, PENDING, CONNECTED, CONNECT_FAILED
  };

  /** action to execute for a peer. */
  public enum action {
    NONE, FIRST_CONNECT, SIMPLE_CONNECT, HANDLE_CONNECT, XQUERY, HANDLE_XQUERY
  }
}
