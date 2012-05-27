package org.basex.core.cmd;

import java.net.*;

import org.basex.core.*;
import org.basex.dist.*;

/**
 * Evaluates the 'distribute' command and opens connections to communicate
 * with other BaseX instances.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class Distribute extends Command {
  /**
   * Open a new network.
   *
   * @param hostIn The host name of the local socket.
   * @param portIn The local socket port number.
   */
  public Distribute(final String hostIn, final String portIn) {
    this(hostIn, portIn, null, null, true);
  }

  /**
   * Connect to a existing network as a normal peer.
   *
   * @param hostIn The host name of the local socket.
   * @param portIn The local socket port number.
   * @param hostOut Connect to this host.
   * @param portOut Connect to this port.
   */
  public Distribute(final String hostIn, final String portIn, final String hostOut,
      final String portOut) {
    this(hostIn, portIn, hostOut, portOut, false);
  }

  /**
   * Default constructor.
   *
   * @param hostIn The host name of the local socket.
   * @param portIn The local socket port number.
   * @param hostOut Connect to this host.
   * @param portOut Connect to this port.
   * @param superPeer Should this peer be a super-peer or not?
   */
  public Distribute(final String hostIn, final String portIn, final String hostOut,
      final String portOut, final boolean superPeer) {
    super(Perm.ADMIN, hostIn, portIn, hostOut, portOut, String.valueOf(superPeer));
  }

  @Override
  protected boolean run() {
    if (context.nNode != null) {
      context.nNode.stop();
    }

    String host = args[0];
    int port = Integer.valueOf(args[1]);

    // connect to network, if given
    if (args[2] != null && args[3] != null) {
      String hostOut = args[2];
      int portOut = Integer.valueOf(args[3]);

      boolean superPeer = args[4].equalsIgnoreCase("true") ? true : false;
      try {
        if (superPeer) {
          context.nNode = new SuperPeer(host, port, context);
        } else {
          context.nNode = new NetworkPeer(host, port, context);
        }
      } catch (UnknownHostException e) {
        return false;
      }

      try {
        if (hostOut != null && portOut > 1023) {
          if (!context.nNode.connectToCluster(InetAddress.getByName(hostOut), portOut))
            return false;
        } else
          return false;
      } catch(UnknownHostException e) {
        return false;
      }
      Thread t = new Thread(context.nNode);
      t.start();


      return true;
    }

    // new network
    try {
      context.nNode = new SuperPeer(host, port, context);
    } catch(UnknownHostException e) {
      return false;
    }
    new Thread(context.nNode).start();

    return true;
  }
}
