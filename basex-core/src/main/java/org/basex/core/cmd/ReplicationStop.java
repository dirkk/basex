package org.basex.core.cmd;

import org.basex.core.Command;
import org.basex.core.Perm;
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
public final class ReplicationStop extends Command {

  /**
   * Constructor.
   */
  public ReplicationStop() {
    super(Perm.ADMIN);
  }

  @Override
  protected boolean run() {
    Replication repl = context.replication;
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
