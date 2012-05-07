package org.basex.core.cmd;

import java.io.IOException;
import org.basex.core.*;
import org.basex.core.Commands.Cmd;
import org.basex.core.Commands.CmdShow;

/**
 * Evaluates the 'show network' command and shows all peers and
 * super-peers, which are known to this peer.
 *
 * @author Dirk Kirsten
 */
public final class ShowNetwork extends Command {
  /**
   * Default constructor.
   */
  public ShowNetwork() {
    super(Perm.ADMIN);
  }

  @Override
  protected boolean run() throws IOException {
    out.println(context.nNode.info());
    return true;
  }

  @Override
  public void build(final CommandBuilder cb) {
    cb.init(Cmd.SHOW + " " + CmdShow.NETWORK);
  }
}
