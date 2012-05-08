package org.basex.dist;

import java.io.*;
import java.net.*;

/**
 * A cluster peer for a super-peer.
 *
 * @author Dirk Kirsten
 *
 */
public class SuperClusterPeer extends ClusterPeer {
  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The connection to talk to this peer.
   */
  public SuperClusterPeer(final NetworkPeer c, final Socket s) {
    super(c, s);
  }

  /**
   * Handle incoming connects from other normal peers
   * and establishes the connection.
   */
  @Override
  protected void handleIncomingConnect() {
    try {
      byte packetIn = in.readByte();
      if(packetIn == DistConstants.P_CONNECT) {
        commandingPeer.addNodeToNetwork(this);
        out.write(DistConstants.P_CONNECT_ACK);

        // Get the host + port of the remote peer for new connection
        // attempts.
        if (in.readByte() != DistConstants.P_CONNECTION_ATTEMPTS)
          return;

        int length = in.readInt();
        byte[] nbHost = new byte[length];
        in.read(nbHost, 0, length);
        connectionHost = InetAddress.getByAddress(nbHost);
        connectionPort = in.readInt();

        // count the number of nodes to send
        int numberPeers = 0;
        for(ClusterPeer n : commandingPeer.nodes.values()) {
          if(n.getStatus() == DistConstants.status.CONNECTED) {
            ++numberPeers;
          }
        }
        out.writeInt(numberPeers);

        for(ClusterPeer n : commandingPeer.nodes.values()) {
          if(n.getStatus() == DistConstants.status.CONNECTED) {
            byte[] bHost = n.getConnectionHostAsByte();
            out.writeInt(bHost.length);
            out.write(bHost, 0, bHost.length);
            out.writeInt(n.getConnectionPort());
          }
        }

        if(in.readByte() == DistConstants.P_CONNECT_NODES_ACK) {
          changeStatus(DistConstants.status.CONNECTED);
        }
      }
    } catch (IOException e) {
      commandingPeer.log.write("I/O error on the socket connection to " +
          getIdentifier());
    }
  }
}
