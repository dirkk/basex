package org.basex.core.cmd;

import static org.basex.core.Text.*;

import java.io.*;

import org.basex.core.parse.*;
import org.basex.core.parse.Commands.*;

/**
 * Evaluates the 'replication start slave' command and starts the
 * replication of this instance as slave, listening to the message broker
 * at the given address.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class ReplicationStartSlave extends AReplication {

  /**
   * Constructor.
   * @param addr message broker address
   * @param name replication set name
   */
  public ReplicationStartSlave(final String addr, final String name) {
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
    
    if (!context.replication.startSlave(addr, name)) {
      return error(R_CONNECTION_REFUSED_X, addr);
    }
    
    return true;
  }
  
  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.REPLICATION + " " + CmdReplication.START_SLAVE).args();
  }
}
