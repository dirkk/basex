package org.basex.core.cmd;

import org.basex.core.Replication;
import org.basex.core.parse.CmdBuilder;
import org.basex.core.parse.Commands.Cmd;
import org.basex.core.parse.Commands.CmdInfo;
import org.basex.util.TokenBuilder;

import java.io.IOException;

/**
 * Evaluates the 'info replication' command and returns information on the
 * currently running replication (master/slave).
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class InfoReplication extends AInfo {
  /**
   * Default constructor.
   */
  public InfoReplication() {
    super(false);
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
  private String replication() {
    final TokenBuilder tb = new TokenBuilder();
    final Replication repl = context.replication;

    if (repl.isEnabled()) {
      repl.info();
      //info(tb, "Running", true);
      // TODO info(tb, "Address", context.replication.getBrokerAddress());
    } else {
      info(tb, "Running", false);
    }
    return tb.toString();
  }

  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.INFO + " " + CmdInfo.REPLICATION);
  }
}
