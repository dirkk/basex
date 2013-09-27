package org.basex.core.cmd;

import org.basex.core.Replication;
import org.basex.core.parse.CmdBuilder;
import org.basex.core.parse.Commands.Cmd;
import org.basex.core.parse.Commands.CmdReplication;
import static org.basex.core.Text.*;
import static org.basex.server.replication.ReplicationExceptions.ReplicationAlreadyRunningException;

/**
 * Evaluates the 'replication connect' command and starts the
 * replication of this instance. This instance can then be a master
 * or slave, depending on the result of the election process.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationConnect extends AReplication {
  /**
   * Constructor.
   * @param host host name
   * @param port port
   */
  public ReplicationConnect(final String host, final String port) {
    super(host, port);
  }

  /**
   * Constructor.
   * @param host host name
   * @param port port
   */
  public ReplicationConnect(final String host, final int port) {
    this(host, String.valueOf(port));
  }

  @Override
  protected boolean run() {
    final String localHost = args[0];
    final int localPort = Integer.valueOf(args[1]);
    final String remoteHost = args[2];
    final int remotePort = Integer.valueOf(args[3]);

    try {
      Replication.getInstance(context).connect(localHost, localPort, remoteHost, remotePort);
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
