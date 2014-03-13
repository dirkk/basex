package org.basex.core.cmd;

import org.basex.core.Command;
import org.basex.core.Perm;
import org.basex.core.parse.CmdBuilder;
import org.basex.core.parse.Commands.Cmd;
import org.basex.core.parse.Commands.CmdReplication;

import static org.basex.core.Text.R_ALREADY_RUNNING;
import static org.basex.server.replication.ReplicationExceptions.ReplicationAlreadyRunningException;

/**
 * Evaluates the 'replication connect' command and starts the
 * replication of this instance. This instance can then be a master
 * or slave, depending on the result of the election process.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationConnect extends Command {
  /**
   * Constructor.
   * @param localHost hostname
   * @param localPort port
   * @param remoteHost remote hostname
   * @param remotePort remote port
   */
  public ReplicationConnect(final String localHost, final String localPort,
                            final String remoteHost, final String remotePort) {
    super(Perm.ADMIN, localHost, localPort, remoteHost, remotePort);
  }

  /**
   * Constructor.
   * @param localHost hostname
   * @param localPort port
   * @param remoteHost remote hostname
   * @param remotePort remote port
   */
  public ReplicationConnect(final String localHost, final int localPort,
                            final String remoteHost, final int remotePort) {
    super(Perm.ADMIN, localHost, String.valueOf(localPort),
      remoteHost, String.valueOf(remotePort));
  }

  @Override
  protected boolean run() {
    final String localHost = args[0];
    final int localPort = Integer.valueOf(args[1]);
    final String remoteHost = args[2];
    final int remotePort = Integer.valueOf(args[3]);

    try {
      context.replication.connect(context, localHost, localPort, remoteHost, remotePort);
    } catch (ReplicationAlreadyRunningException e) {
      return error(R_ALREADY_RUNNING);
    }
    return true;
  }
  
  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.REPLICATION + " " + CmdReplication.CONNECT).args();
  }
}
