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
  /** Commanding network-peer. */
  protected SuperPeer commandingSuperPeer;

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The connection to talk to this peer.
   */
  public SuperClusterPeer(final SuperPeer c, final Socket s) {
    super(c, s);

    commandingSuperPeer = c;
  }

  /**
   * Default constructor.
   * @param c The commanding peer for this peer in the cluster.
   * @param s The connection to talk to this peer.
   * @param superpeer is this a super-peer?
   */
  public SuperClusterPeer(final SuperPeer c, final Socket s, final boolean superpeer) {
    super(c, s, superpeer);

    commandingSuperPeer = c;
  }

  /**
   * Handles the connection establishment with a new joining
   * normal peer.
   */
  @Override
  protected void handleConnectFromNormalpeer() {
    try {
      commandingPeer.addPeerToNetwork(this);
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
    } catch (IOException e) {
      commandingPeer.log.write("I/O error on the socket connection to " +
          getIdentifier());
    }
  }

  /**
   * Another super-peer tries to connect to this super-peer,
   * so this handles the connection establishment.
   */
  @Override
  protected void handleConnectFromSuperpeer() {
    try {
      out.write(DistConstants.P_CONNECT_ACK);

      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectionHost = InetAddress.getByAddress(nbHost);
      connectionPort = in.readInt();

      // count the number of nodes to send
      int numberPeers = 0;
      for(ClusterPeer n : commandingSuperPeer.superPeers.values()) {
        if(n.getStatus() == DistConstants.status.CONNECTED) {
          ++numberPeers;
        }
      }
      out.writeInt(numberPeers);

      for(ClusterPeer n : commandingSuperPeer.superPeers.values()) {
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
    } catch (IOException e) {
      commandingPeer.log.write("I/O exception.");
    }
  }

  /**
   * Connect to another super-peer to join the network.
   *
   * @return success
   */
  @Override
  protected boolean connect() {
    try {
        byte[] bHost = commandingPeer.host.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.port);

        int nnodes = in.readInt();
        for (int i = 0; i < nnodes; ++i) {
          int length = in.readInt();
          byte[] nbHost = new byte[length];
          in.read(nbHost, 0, length);
          InetAddress cHost = InetAddress.getByAddress(nbHost);
          int cPort = in.readInt();
          commandingPeer.connectToPeer(cHost, cPort);
        }

        out.write(DistConstants.P_CONNECT_NODES_ACK);
        status = DistConstants.status.CONNECTED;
        return true;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }
  }
}
