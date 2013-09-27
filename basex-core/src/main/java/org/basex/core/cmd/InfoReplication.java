package org.basex.core.cmd;

import java.io.*;

import org.basex.core.*;
import org.basex.core.parse.*;
import org.basex.core.parse.Commands.*;
import org.basex.util.*;

/**
 * Evaluates the 'info replication' command and returns information on the
 * currently running replication (master/slave).
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class InfoReplication extends AQuery {
  /**
   * Default constructor.
   */
  public InfoReplication() {
    super(Perm.ADMIN, false);
  }

  @Override
  protected boolean run() throws IOException {
    out.print(replication());
    return true;
  }

  /**
   * Creates a replication information string.
   * @return info string
   */
  public static String replication() {

    final TokenBuilder tb = new TokenBuilder();
    //info(tb, NAME, meta.name);
    return tb.toString();
  }

  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.INFO + " " + CmdInfo.REPLICATION);
  }
}
