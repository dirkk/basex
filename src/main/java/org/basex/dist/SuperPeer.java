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
   * Constructor by using a normal peer.
   * @param peer A normal peer.
   */
  public SuperPeer(final NetworkPeer peer) {
    super(peer);

    superPeer = null;
    superPeers = new LinkedHashMap<String, ClusterPeer>();
  }

  /**
   * Join a BaseX network.. Connect to any one of the super-peers.
   *
   * @param cHost the name of the peer to connect to.
   * @param cPort the port number of the peer to connect to.
   */
  @Override
  protected void connectTo(final InetAddress cHost, final int cPort) {
    // open socket (if needed) for outgoing packets
    if(socketOut == null || cHost != socketOut.getInetAddress()
        || cPort != socketOut.getPort()) {
      try {
        //TODO synchronized!!
        socketOut = new Socket(cHost, cPort, host, port + nodes.values().size() + 2);
        out = new DataOutputStream(socketOut.getOutputStream());
        DataInputStream inFromRemote = new DataInputStream(socketOut.getInputStream());

        out.write(DistConstants.P_CONNECT_SUPER);

        byte packetIn = inFromRemote.readByte();
        if(packetIn == DistConstants.P_CONNECT_ACK) {
          SuperClusterPeer spc = new SuperClusterPeer(this, socketOut, true);
          new Thread(spc).start();
          spc.doConnect = true;
        } else if (packetIn == DistConstants.P_SUPERPEER_ADDR) {
            int length = inFromRemote.readInt();
            byte[] nHost = new byte[length];
            inFromRemote.read(nHost, 0, length);
            connectTo(InetAddress.getByAddress(nHost), inFromRemote.readInt());
          }
      } catch(BindException e) {
        log.write("Could not bind to this address.");
        log.write(e.getMessage());
      } catch(IOException e) {
        log.write("I/O error while trying to connect to the node cluster.");
      }
    }
  }

  /**
   * Connects this super-peer with the given super-peer.
   *
   * @param cHost The host name to connect to
   * @param cPort The port number to connect to.
   * @return success.
   */
  @Override
  public boolean connectToPeer(final InetAddress cHost, final int cPort) {
    try {
      Socket s = new Socket(cHost, cPort, host,
          port + nodes.values().size() + 2);
      ClusterPeer newPeer = new ClusterPeer(this, s);

      addSuperPeerToNetwork(newPeer);
      new Thread(newPeer).start();
      newPeer.doConnect = true;
      return true;
    } catch (IOException e) {
      log.write("Could not connect to peer " + cHost.getHostAddress());
      return false;
    }
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

        SuperClusterPeer cn = new SuperClusterPeer(this, socketIn);
        Thread t = new Thread(cn);
        t.start();
        cn.doHandleConnect = true;
      } catch (IOException e) {
        log.write("I/O socket error.");
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
