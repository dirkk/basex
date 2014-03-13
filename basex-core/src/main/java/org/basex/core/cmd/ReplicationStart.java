package org.basex.core.cmd;

import org.basex.core.Command;
import org.basex.core.Perm;
import org.basex.core.Replication;
import org.basex.core.parse.CmdBuilder;
import org.basex.core.parse.Commands.Cmd;
import org.basex.core.parse.Commands.CmdReplication;

import static org.basex.core.Text.R_ALREADY_RUNNING;
import static org.basex.core.Text.R_CONNECTION_REFUSED_X;
import static org.basex.server.replication.ReplicationExceptions.ReplicationAlreadyRunningException;

/**
 * Evaluates the 'replication start master' command and starts this
 * instance as a master for replication.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStart extends Command {
  /**
   * Constructor.
   * @param host host name to listen to
   * @param port port to listen to
   */
  public ReplicationStart(final String host, final String port) {
    super(Perm.ADMIN, host, port);
  }

  /**
   * Constructor.
   * @param host host name to listen to
   * @param port port to listen to
   */
  public ReplicationStart(final String host, final int port) {
    this(host, String.valueOf(port));
  }

  @Override
  protected boolean run() {
    final String host = args[0];
    final int port = Integer.valueOf(args[1]);
    final Replication repl = context.replication;

    if (repl.isEnabled()) {
      return error(R_ALREADY_RUNNING);
    }

    try {
      if (!repl.start(context, host, port)) {
        return error(R_CONNECTION_REFUSED_X, host + ":" + port);
      }
    } catch (ReplicationAlreadyRunningException e) {
      return error(R_ALREADY_RUNNING);
    }

    return true;
  }
  
  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.REPLICATION + " " + CmdReplication.START).args();
  }
}
