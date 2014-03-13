package org.basex.server.replication;

import org.basex.core.*;
import org.basex.io.out.NullOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Facade to execute a number of commands as one transaction.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ReplCmd extends Command {
  /** Commands to execute. */
  private final ArrayList<Command> cmds;
  /** Database context. */
  private final Context dbCtx;

  /**
   * Constructor.
   * @param cmds commands to execute
   */
  public ReplCmd(final ArrayList<Command> cmds, final Context dbCtx) {
    super(maxPerm(cmds));

    this.cmds = cmds;
    this.dbCtx = dbCtx;
  }

  /**
   * Returns the maximum permission from the specified commands.
   * @param cmds commands to be checked
   * @return permission
   */
  private static Perm maxPerm(final List<Command> cmds) {
    Perm p = Perm.NONE;
    for(final Command c : cmds) p = p.max(c.perm);
    return p;
  }

  @Override
  public void databases(final LockResult lr) {
    for(final Command c : cmds) c.databases(lr);

    // lock globally if context-dependent is found (context will be changed by commands)
    final boolean wc = lr.write.contains(DBLocking.CTX) || lr.write.contains(DBLocking.COLL);
    final boolean rc = lr.read.contains(DBLocking.CTX) || lr.read.contains(DBLocking.COLL);
    if(wc || rc && !lr.write.isEmpty()) {
      lr.writeAll = true;
      lr.readAll = true;
    } else if(rc) {
      lr.readAll = true;
    }
  }

  @Override
  public boolean updating(final Context ctx) {
    boolean up = true;
    for(final Command c : cmds) up |= c.updating(ctx);
    return up;
  }

  @Override
  public final boolean run() {
    try {
      System.out.println("Run " + Thread.currentThread().getId());
      for(final Command c : cmds) c.run(dbCtx, new NullOutput());
      return true;
    } finally {
      //new Close().run(dbCtx);
    }
  }
}
