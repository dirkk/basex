package org.basex.dist;

import static org.basex.core.Text.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.basex.core.*;
import org.basex.util.*;

/**
 * A super-peer within the network. Acts as superior
 * within one cluster (one super-peer, indefinite number of
 * normal peers), but as equal among other super-peers.
 *
 * @author Dirk Kirsten
 */
public class SuperPeer extends NetworkPeer {
  /** All super-peers in the network. */
  public Map<String, ClusterPeer> superPeers;

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
   * Adds a new super-peer to the network cluster. This is just used for the internal
   * table of this peer and has no effect on the recognition of this node in the
   * overall network.
   * @param cp The peer to add
   */
  protected void addSuperPeerToNetwork(final ClusterPeer cp) {
    superPeers.put(cp.getIdentifier(), cp);
  }

  /**
   * Join a BaseX network. Connect to any one of the super-peers.
   *
   * @param cHost the name of the peer to connect to.
   * @param cPort the port number of the peer to connect to.
   */
  @Override
  public boolean connectToCluster(final InetAddress cHost, final int cPort) {
    SuperClusterPeer spc = new SuperClusterPeer(this, host,
        getNextFreePort(), cHost, cPort, true);
    spc.actionType = DistConstants.action.FIRST_CONNECT;

    return connectClusterPeer(spc);
  }

  /**
   * Connects this super-peer with the given super-peer.
   *
   * @param cHost The host name to connect to
   * @param cPort The port number to connect to.
   * @return success.
   */
  @Override
  public synchronized boolean connectToPeer(final InetAddress cHost, final int cPort) {
      SuperClusterPeer pc = new SuperClusterPeer(this, host, getNextFreePort(), cHost,
          cPort, true);
      pc.actionType = DistConstants.action.SIMPLE_CONNECT;

      return connectClusterPeer(pc);
  }

  /**
   * Returns the next free port, starting from the initial port of
   * this peer.
   *
   * @return next free port.
   */
  @Override
  protected int getNextFreePort() {
    return port + peers.values().size() + superPeers.size() + 1;
  }

  @Override
  public void run() {
    while(true) {
      try {
        /* Waiting for another node to connect */
        try {
          Socket socketIn = serverSocket.accept();
          socketIn.setReuseAddress(true);

          SuperClusterPeer cn = new SuperClusterPeer(this, socketIn);
          Thread t = new Thread(cn);
          t.start();
        } catch(SocketTimeoutException e) {
          continue;
        }
      } catch (IOException e) {
        Util.outln(D_SOCKET_WAIT_ERROR_X, serverSocket.getInetAddress().toString()
            + serverSocket.getLocalPort());
      }
    }
  }

  /**
   * Gives information about this peer, connected normal peers and
   * connected super-peers.
   * @return information about this peer.
   */
  @Override
  public String info() {
    String o = new String("Super peer\r\n\r\nThis cluster:\r\n");
    o += getIdentifier() + "\r\n";
    for(ClusterPeer c : peers.values()) {
      o += "|--- " + c.getIdentifier() + "\r\n";
    }

    o += "\r\nSuper-Peers:\r\n";
    for(ClusterPeer c : superPeers.values()) {
      o += c.getIdentifier() + "\r\n";
    }

    return o;
  }
}
