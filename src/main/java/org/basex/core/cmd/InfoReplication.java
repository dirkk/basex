package org.basex.core.cmd;

import static org.basex.core.Text.*;

import java.io.*;
import java.net.*;

import org.basex.core.parse.*;
import org.basex.core.parse.Commands.*;
import org.basex.util.*;

/**
 * Evaluates the 'info replication' command and connects as a slave to
 * an already existing master.
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
    out.print(buildInfo());
    return true;
  }
  
  /**
   * Builds the info string for replication.
   * @return info string
   */
  private String buildInfo() {
    final TokenBuilder tb = new TokenBuilder();
    
    tb.add("General Information").add(NL);
    if (context.repl == null) {
      info(tb, "Activated", false);
      return tb.toString();
    }
    
    if (context.repl.isMaster()) {
      info(tb, "Activated", true);
      info(tb, MASTER + "/" + SLAVE, MASTER);
      for (InetSocketAddress addr : context.repl.slaveAddresses())
        info(tb, SLAVE, addr.toString());
    } else if (context.repl.isSlave()) {
      info(tb, "Activated", true);
      // this is a slave node
      try {
        Thread.sleep(100);
      } catch(InterruptedException e) {
        e.printStackTrace();
      }
      info(tb, MASTER + "/" + SLAVE, SLAVE);
      info(tb, MASTER, context.repl.masterAddress());
    } else {
      info(tb, "Activated", false);
      return tb.toString();
    }
    
    return tb.toString();
  }
  
  @Override
  public void build(final CmdBuilder cb) {
    cb.init(Cmd.INFO + " " + CmdInfo.REPLICATION);
  }
}
