package org.basex.core.cmd;

import org.basex.core.*;

/**
 * Evaluates the 'replication start master' command and starts this
 * instance as a master for replication.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStartMaster extends Command {
  /**
   * Constructor.
   * @param addr message broker address in the format host:port
   */
  public ReplicationStartMaster(final String addr) {
    super(Perm.ADMIN, false, addr);
  }

  @Override
  protected boolean run() {
    final String addrString = args[0];

    return true;
  }
}
