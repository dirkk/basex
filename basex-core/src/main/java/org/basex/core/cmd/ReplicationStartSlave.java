package org.basex.core.cmd;

import org.basex.core.*;

/**
 * Evaluates the 'replication start slave' command and starts the
 * replication of this instance as slave, listening to the message broker
 * at the given address.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStartSlave extends Command {

  /**
   * Constructor.
   * @param addr message broker address
   */
  public ReplicationStartSlave(final String addr) {
    super(Perm.ADMIN, false, addr);
  }

  @Override
  protected boolean run() {
    final String addrString = args[0];

    return true;
  }
}
