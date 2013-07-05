package org.basex.core.cmd;

import org.basex.core.*;

/**
 * Evaluates the 'replicate start' command and enable this node to act as
 * master.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplStart extends Command {
  /**
   * Default constructor.
   */
  public ReplStart() {
    super(Perm.ADMIN, false);
  }

  @Override
  protected boolean run() {
    if (context == null || context.repl == null)
      return false;
    
    return context.repl.start();
  }
}
