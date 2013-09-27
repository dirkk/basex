package org.basex.core.cmd;

import org.basex.core.Replication;
import org.basex.core.parse.*;
import org.basex.core.parse.Commands.*;

/**
 * Evaluates the 'replication stop' command and stops the
 * replication of this instance.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStop extends AReplication {

  /**
   * Constructor.
   */
  public ReplicationStop() {
    super();
  }

  @Override
  protected boolean run() {
    Replication repl = Replication.getInstance(context);
    if (repl.isEnabled()) {
      repl.stop();
      return true;
    }
    
    return false;
  }
  
  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.REPLICATION + " " + CmdReplication.STOP).args();
  }
}
