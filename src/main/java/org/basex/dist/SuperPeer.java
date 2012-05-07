package org.basex.dist;

import java.io.*;
import java.net.*;
import java.util.*;

import org.basex.core.*;

/**
 * A super-peer within the network. Acts as superior
 * within one cluster (one super-peer, indefinite number of
 * normal peers), but as equal among other super-peers.
 *
 * @author Dirk Kirsten
 */
public class SuperPeer extends NetworkPeer {
  /** All super-peers in the network. */
  private Map<String, ClusterPeer> superPeers;

  /**
   * Default constructor.
   * @param nHost the local host name.
   * @param nPort the local port number.
   * @param context Database context.
   * @throws UnknownHostException unknown host
   */
  public SuperPeer(final String nHost, final int nPort, final Context context)
      throws UnknownHostException {
    super(nHost, nPort, context);

    superPeer = null;
    superPeers = new LinkedHashMap<String, ClusterPeer>();
  }

  /**
   * Constructor by using a normal peer.
   * @param peer A normal peer.
   */
  public SuperPeer(final NetworkPeer peer) {
    super(peer);

    superPeer = null;
    superPeers = new LinkedHashMap<String, ClusterPeer>();
  }

  @Override
  public boolean connectTo(final InetAddress cHost, final int cPort) {
    // TODO everything
    return true;
  }

  @Override
  public void run() {
    while(true) {
      try {
        /* Waiting for another node to connect */
        try {
          socketIn = serverSocket.accept();
        } catch(SocketTimeoutException e) {
          continue;
        }

        in = new DataInputStream(socketIn.getInputStream());
        DataOutputStream cOut = new DataOutputStream(socketIn.getOutputStream());

        byte packetIn = in.readByte();
        if(packetIn == DistConstants.P_CONNECT) {
          InetAddress newHost = socketIn.getInetAddress();
          int newPort = socketIn.getPort() - 1;
          ClusterPeer cn = new ClusterPeer(newHost, newPort);

          addNodeToNetwork(cn);
          cOut.write(DistConstants.P_CONNECT_ACK);

          if(in.readByte() == DistConstants.P_SUPERPEER_NEAREST) {
            cOut.write(DistConstants.P_SUPERPEER_ME);

            // send all nodes to the new node
            cOut.write(DistConstants.P_CONNECT_NODES);

            // count the number of nodes to send
            int numberPeers = 0;
            for(ClusterPeer n : nodes.values()) {
              if(n.getStatus() == DistConstants.status.CONNECTED) {
                ++numberPeers;
              }
            }
            cOut.writeInt(numberPeers);

            for(ClusterPeer n : nodes.values()) {
              if(n.getStatus() == DistConstants.status.CONNECTED) {
                byte[] bHost = n.getHostAsByte();
                cOut.writeInt(bHost.length);
                cOut.write(bHost, 0, bHost.length);
                cOut.writeInt(n.port);
              }
            }

            if(in.readByte() == DistConstants.P_CONNECT_NODES_ACK) {
              cn.changeStatus(DistConstants.status.CONNECTED);
              propagateNewPeer(cn);
            }
          }

        } else if(packetIn == DistConstants.P_CONNECT_SUPER) {
          String newHost = socketIn.getRemoteSocketAddress().toString();
          int newPort = socketIn.getPort();
          cOut.write(DistConstants.P_CONNECT_ACK);

          if(packetIn == DistConstants.P_SUPERPEER_ADDR) {
            cOut.write(DistConstants.P_SUPERPEER_ME);
          }

          ClusterPeer cn = new ClusterPeer(newHost, newPort);
          addSuperPeerToNetwork(cn);

          // send all super-peers to the new super-peer
          cOut.write(DistConstants.P_CONNECT_NODES);
          cOut.writeInt(superPeers.size());
          for(ClusterPeer n : superPeers.values()) {
            byte[] bHost = n.getHostAsByte();
            cOut.writeInt(bHost.length);
            cOut.write(bHost, 0, bHost.length);
            cOut.writeInt(n.port);
          }

          if(in.readByte() == DistConstants.P_CONNECT_ACK) {
            cn.changeStatus(DistConstants.status.CONNECTED);
            propagateNewPeer(cn);
          }
        }
      } catch(IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Adds a new super-peer to the network cluster. This is just used for the internal
   * table of this peer and has no effect on the recognition of this peer in the overall
   * network.
   *
   * @param cn The super-peer to add.
   */
  protected void addSuperPeerToNetwork(final ClusterPeer cn) {
    cn.changeStatus(DistConstants.status.PENDING);
    superPeers.put(cn.getIdentifier(), cn);
  }

  /**
   * Gives information about this peer and connected peers.
   * @return information about this peer.
   */
  @Override
  public String info() {
    String o = new String("Super peer\r\n\r\nThis cluster:\r\n");
    o += getIdentifier() + "\r\n";
    for(ClusterPeer c : nodes.values()) {
      o += "|--- " + c.getIdentifier() + "\r\n";
    }

    o += "\r\nSuper-Peers:\r\n";
    for(ClusterPeer c : superPeers.values()) {
      o += c.getIdentifier() + "\r\n";
    }

    return o;
  }
}
