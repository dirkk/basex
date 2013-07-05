package org.basex.core.cmd;

import java.net.*;

import org.basex.core.*;

/**
 * Evaluates the 'replicate connect' command and connects as a slave to
 * an already existing master.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplConnect extends Command {
  /**
   * Default constructor.
   * @param master master address in the format host:port
   */
  public ReplConnect(final String master) {
    super(Perm.ADMIN, false, master);
  }

  @Override
  protected boolean run() {
    String[] split = args[0].split(":");
    String host = split[0];
    int port = Integer.valueOf(split[1]);
    InetSocketAddress master;
    try {
      master = new InetSocketAddress(InetAddress.getByName(host), port);
      return context.repl.connect(master);
    } catch(UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return false;
    }
  }
}
