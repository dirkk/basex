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
   * Adds a new super-peer to the network cluster. This is just used for the internal
   * table of this peer and has no effect on the recognition of this node in the
   * overall network.
   * @param cp The peer to add
   */
  protected void addSuperPeerToNetwork(final ClusterPeer cp) {
    superPeers.put(cp.getIdentifier(), cp);
    cp.changeStatus(DistConstants.status.CONNECTED);
  }

  /**
   * Join a BaseX network. Connect to any one of the super-peers.
   *
   * @param cHost the name of the peer to connect to.
   * @param cPort the port number of the peer to connect to.
   */
  @Override
  protected void connectTo(final InetAddress cHost, final int cPort) {
    try {
      Socket socketOut = new Socket(cHost, cPort, host, port + nodes.values().size() + 1);
      socketOut.setReuseAddress(true);
      out = new DataOutputStream(socketOut.getOutputStream());
      DataInputStream inNow = new DataInputStream(socketOut.getInputStream());

      out.write(DistConstants.P_CONNECT_SUPER);

      byte packetIn = inNow.readByte();
      if(packetIn == DistConstants.P_CONNECT_ACK) {
        SuperClusterPeer spc = new SuperClusterPeer(this, socketOut, true);
        spc.doFirstConnect = true;
        new Thread(spc).start();
        spc.lock.lock();
        spc.action.signalAll();
        spc.lock.unlock();
      } else if (packetIn == DistConstants.P_SUPERPEER_ADDR) {
          int length = inNow.readInt();
          byte[] nHost = new byte[length];
          inNow.read(nHost, 0, length);
          connectTo(InetAddress.getByAddress(nHost), inNow.readInt());
        }
    } catch(BindException e) {
      log.write("Could not bind to this address.");
      log.write(e.getMessage());
    } catch(IOException e) {
      log.write("I/O error while trying to connect to the node cluster.");
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
          port + nodes.values().size() + superPeers.size() + 3);
      s.setReuseAddress(true);
      out = new DataOutputStream(s.getOutputStream());
      DataInputStream inNow = new DataInputStream(s.getInputStream());

      out.write(DistConstants.P_CONNECT_SUPER);

      if (inNow.readByte() == DistConstants.P_CONNECT_ACK) {
        ClusterPeer pc = new ClusterPeer(this, s, true);
        addSuperPeerToNetwork(pc);
        pc.doSimpleConnect = true;
        new Thread(pc).start();
        pc.lock.lock();
        pc.action.signalAll();
        pc.lock.unlock();
        return true;
      }

      return false;
    } catch (IOException e) {
      log.write("Could not connect to peer " + cHost.getHostAddress());
      return false;
    }
  }

  @Override
  public void run() {
    if (connectHost != null && connectPort > 1023)
      connectTo(connectHost, connectPort);

    while(true) {
      try {
        /* Waiting for another node to connect */
        try {
          socketIn = serverSocket.accept();
          socketIn.setReuseAddress(true);
        } catch(SocketTimeoutException e) {
          continue;
        }

        SuperClusterPeer cn = new SuperClusterPeer(this, socketIn);
        Thread t = new Thread(cn);
        cn.doHandleConnect = true;
        t.start();
        cn.lock.lock();
        cn.action.signalAll();
        cn.lock.unlock();
      } catch (IOException e) {
        log.write("I/O socket error.");
      }
    }
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
