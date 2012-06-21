package org.basex.dist;

import static org.basex.core.Text.*;

import java.io.*;
import java.net.*;

import org.basex.util.*;

/**
 * A cluster peer for a super-peer. This class represents another peer (super-peer or
 * not) in the network.
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
   * @param nHost host name to bind to.
   * @param nPort port number to bind to.
   * @param nConnHost host name to connect to.
   * @param nConnPort port number to connect to.
   * @param newSuperPeer is this a super-peer?
   */
  public SuperClusterPeer(final SuperPeer c, final InetAddress nHost, final int nPort,
      final InetAddress nConnHost, final int nConnPort, final boolean newSuperPeer) {
    super(c, nHost, nPort, nConnHost, nConnPort, newSuperPeer);

    commandingSuperPeer = c;
  }

  /**
   * Handles the connection establishment with a new joining
   * normal peer.
   */
  @Override
  protected void handleConnectFromNormalpeer() {
    try {
      state = DistConstants.state.PENDING;
      commandingSuperPeer.addPeerToNetwork(this);
      out.write(DistConstants.P_CONNECT_ACK);

      byte[] sendHost = commandingSuperPeer.serverSocket.getInetAddress().getAddress();
      out.writeInt(sendHost.length);
      out.write(sendHost);
      out.writeInt(commandingSuperPeer.serverSocket.getLocalPort());

      byte packetIn = in.readByte();
      if (packetIn == DistConstants.P_CONNECT_SEND_PEERS) {
        int length = in.readInt();
        byte[] nbHost = new byte[length];
        in.read(nbHost);
        connectHost = InetAddress.getByAddress(nbHost);
        connectPort = in.readInt();

        // count the number of super-peers to send
        int numberPeers = 0;
        for(ClusterPeer n : commandingPeer.peers.values()) {
          if(n.getStatus() == DistConstants.state.CONNECTED) {
            ++numberPeers;
          }
        }
        out.writeInt(numberPeers);

        for(ClusterPeer n : commandingPeer.peers.values()) {
          if(n.getStatus() == DistConstants.state.CONNECTED) {
            byte[] bHost = n.getConnectionHostAsByte();
            out.writeInt(bHost.length);
            out.write(bHost);
            out.writeInt(n.getConnectionPort());
          }
        }

        if (in.readByte() == DistConstants.P_CONNECT_NODES_ACK) {
          changeStatus(DistConstants.state.CONNECTED);
        } else {
          changeStatus(DistConstants.state.CONNECT_FAILED);
        }
      } else if(packetIn == DistConstants.P_CONNECT_NORMAL) {
        out.write(DistConstants.P_CONNECT_NORMAL_ACK);
        changeStatus(DistConstants.state.CONNECTED);
      }
    } catch (IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  /**
   * Another super-peer tries to connect to this super-peer,
   * so this handles the connection establishment.
   */
  @Override
  protected void handleConnectFromSuperpeer() {
    try {
      state = DistConstants.state.PENDING;
      commandingSuperPeer.addSuperPeerToNetwork(this);
      out.write(DistConstants.P_CONNECT_ACK);

      byte[] sendHost = commandingSuperPeer.serverSocket.getInetAddress().getAddress();
      out.writeInt(sendHost.length);
      out.write(sendHost);
      out.writeInt(commandingSuperPeer.serverSocket.getLocalPort());

      byte packetIn = in.readByte();
      if (packetIn == DistConstants.P_CONNECT_SEND_PEERS) {
        int length = in.readInt();
        byte[] nbHost = new byte[length];
        in.read(nbHost);
        connectHost = InetAddress.getByAddress(nbHost);
        connectPort = in.readInt();

        // count the number of nodes to send
        int numberPeers = 0;
        for(ClusterPeer n : commandingSuperPeer.superPeers.values()) {
          if(n.getStatus() == DistConstants.state.CONNECTED) {
            ++numberPeers;
          }
        }
        out.writeInt(numberPeers);

        for (ClusterPeer n : commandingSuperPeer.superPeers.values()) {
          if (n.getStatus() == DistConstants.state.CONNECTED) {
            byte[] bHost = n.getConnectionHostAsByte();
            out.writeInt(bHost.length);
            out.write(bHost);
            out.writeInt(n.getConnectionPort());
          }
        }

        if (in.readByte() == DistConstants.P_CONNECT_NODES_ACK) {
          changeStatus(DistConstants.state.CONNECTED);
        } else {
          changeStatus(DistConstants.state.CONNECT_FAILED);
        }
      } else if(packetIn == DistConstants.P_CONNECT_NORMAL) {
        changeStatus(DistConstants.state.CONNECTED);
        out.write(DistConstants.P_CONNECT_NORMAL_ACK);
      }
    } catch (IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  /**
   * Connect to another super-peer to join the network.
   */
  @Override
  protected void initiateConnect() {
    try {
      state = DistConstants.state.PENDING;
      out.write(DistConstants.P_CONNECT_SUPER);
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
      changeStatus(DistConstants.state.CONNECT_FAILED);
    }
  }

  @Override
  protected void connect() {
    try {
      commandingSuperPeer.addSuperPeerToNetwork(this);

      int length = in.readInt();
      byte[] nbHost = new byte[length];
      in.read(nbHost, 0, length);
      connectHost = InetAddress.getByAddress(nbHost);
      connectPort = in.readInt();

      if (commandingSuperPeer.superPeers.size() <= 1) {
        out.write(DistConstants.P_CONNECT_SEND_PEERS);

        byte[] bHost = commandingPeer.host.getAddress();
        out.writeInt(bHost.length);
        out.write(bHost, 0, bHost.length);
        out.writeInt(commandingPeer.port);

        int nnodes = in.readInt();
        for(int i = 0; i < nnodes; ++i) {
          int length2 = in.readInt();
          byte[] nbHost2 = new byte[length2];
          in.read(nbHost2, 0, length);
          InetAddress cHost = InetAddress.getByAddress(nbHost2);
          int cPort = in.readInt();
          if (commandingSuperPeer.connectToPeer(cHost, cPort)) {
            changeStatus(DistConstants.state.DISCONNECTED);
            return;
          }
        }

        out.write(DistConstants.P_CONNECT_NODES_ACK);
        changeStatus(DistConstants.state.CONNECTED);
      } else {
        out.write(DistConstants.P_CONNECT_NORMAL);
        if(in.readByte() == DistConstants.P_CONNECT_NORMAL_ACK) {
          changeStatus(DistConstants.state.CONNECTED);
        } else {
          changeStatus(DistConstants.state.CONNECT_FAILED);
        }
      }
    } catch(IOException e) {
      Util.outln(D_SOCKET_CLOSED_X, socket.getInetAddress().toString()
          + socket.getPort());
    }
  }

  @Override
  protected void initiateSimpleConnect() {
    initiateConnect();
  }
}
