package org.basex.core.cmd;

import java.io.*;
import java.net.*;

import org.basex.core.*;
import org.basex.dist.*;
import org.basex.util.*;

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
   * @param host The host name of the local socket.
   * @param port The local socket port number.
   */
  public Distribute(final String host, final String port) {
    this(host, port, null, null);
  }

  /**
   * Connect to a existing network as a normal peer.
   *
   * @param host The host name of the local socket.
   * @param port The local socket port number.
   * @param remoteHost Connect to this host.
   * @param remotePort Connect to this port.
   */
  public Distribute(final String host, final String port, final String remoteHost,
      final String remotePort) {
    super(Perm.ADMIN, host, port, remoteHost, remotePort);
  }

  @Override
  protected boolean run() {
    if (context.nNode != null) {
      context.nNode.stop();
    }

    String host = args[0];
    int port = Integer.valueOf(args[1]);

    // open local sockets
    try {
      context.nNode = new NetworkPeer(host, port, context);
    } catch (IOException e) {
      Util.errln("Could not open new network. Cause: ");
      e.printStackTrace();
      return false;
    }
    
    // connect to network, if given
    if (args[2] != null && args[3] != null) {
      String remoteHost = args[2];
      int remotePort = Integer.valueOf(args[3]);

      if (remoteHost != null && remotePort > 1023) {
        context.nNode.connect(new InetSocketAddress(remoteHost, remotePort));
      } else {
        return false;
      }
    }

    new Thread(context.nNode).start();

    return true;
  }
}
