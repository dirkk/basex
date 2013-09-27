package org.basex.core.cmd;

import org.basex.core.*;

/**
 * Evaluates the 'replication stop' command and stops the
 * replication of this instance.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStop extends Command {

  /**
   * Constructor.
   */
  public ReplicationStop() {
    super(Perm.ADMIN, false);
  }

  @Override
  protected boolean run() {
    return true;
  }
}
