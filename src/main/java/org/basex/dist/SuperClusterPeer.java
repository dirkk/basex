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
      out.write(DistConstants.P_CONNECT_ACK);

      byte[] sendHost = commandingSuperPeer.serverSocket.getInetAddress().getAddress();
      out.writeInt(sendHost.length);
      out.write(sendHost);
      out.writeInt(commandingSuperPeer.serverSocket.getLocalPort());

      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectionHost = InetAddress.getByAddress(nbHost);
      connectionPort = in.readInt();

      // count the number of super-peers to send
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
        commandingPeer.addPeerToNetwork(this);
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

      byte[] sendHost = commandingSuperPeer.serverSocket.getInetAddress().getAddress();
      out.writeInt(sendHost.length);
      out.write(sendHost);
      out.writeInt(commandingSuperPeer.serverSocket.getLocalPort());

      byte packetIn = in.readByte();
      if (packetIn == DistConstants.P_CONNECT_SEND_SUPERPEERS) {
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
          commandingSuperPeer.addSuperPeerToNetwork(this);
        }
      } else if(packetIn == DistConstants.P_CONNECT_NORMAL) {
        commandingSuperPeer.addSuperPeerToNetwork(this);
        out.write(DistConstants.P_CONNECT_NORMAL_ACK);
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
        int length = in.readInt();
        byte[] nbHost = new byte[length];
        in.read(nbHost, 0, length);
        connectionHost = InetAddress.getByAddress(nbHost);
        connectionPort = in.readInt();

        out.write(DistConstants.P_CONNECT_SEND_SUPERPEERS);

        byte[] bHost = commandingPeer.host.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.port);

        int nnodes = in.readInt();
        for (int i = 0; i < nnodes; ++i) {
          int length2 = in.readInt();
          byte[] nbHost2 = new byte[length2];
          in.read(nbHost2, 0, length);
          InetAddress cHost = InetAddress.getByAddress(nbHost2);
          int cPort = in.readInt();
          commandingSuperPeer.connectToPeer(cHost, cPort);
        }

        out.write(DistConstants.P_CONNECT_NODES_ACK);
        commandingSuperPeer.addSuperPeerToNetwork(this);
        return true;
    } catch(IOException e) {
      commandingPeer.log.write("I/O error");
      return false;
    }
  }
}
