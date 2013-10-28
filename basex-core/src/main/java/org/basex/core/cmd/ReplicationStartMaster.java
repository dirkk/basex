package org.basex.core.cmd;

import static org.basex.core.Text.*;

import java.io.*;

import org.basex.core.parse.*;
import org.basex.core.parse.Commands.*;

/**
 * Evaluates the 'replication start master' command and starts this
 * instance as a master for replication.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStartMaster extends AReplication {
  /**
   * Constructor.
   * @param addr message broker address in the format host:port
   * @param name replication set name
   */
  public ReplicationStartMaster(final String addr, final String name) {
    super(addr, name);
  }

  @Override
  protected boolean run() {
    final String addr;
    try {
      addr = parseAMQP(args[0]);
    } catch(IOException e) {
      return error(R_INVALID_ADDRESS);
    }
    final String name = args[1];
    
    if (context.replication.isRunning()) {
      return error(R_ALREADY_RUNNING);
    }
    
    if (!context.replication.startMaster(addr, name)) {
      return error(R_CONNECTION_REFUSED_X, addr);
    }
    
    return true;
  }
  
  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.REPLICATION + " " + CmdReplication.START_MASTER).args();
  }
}
