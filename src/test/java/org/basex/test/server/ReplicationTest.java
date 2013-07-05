package org.basex.test.server;

import static org.junit.Assert.*;
import static org.basex.core.Text.*;

import java.io.*;

import org.basex.*;
import org.basex.server.client.*;
import org.basex.test.*;
import org.basex.util.*;
import org.junit.*;

/**
 * This class tests the replication feature of the client/server
 * infrastructure.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ReplicationTest extends SandboxTest {
  /**
   * Test the startup of one master and one connected
   * slave node.
   * @throws IOException I/O exception
   */
  @Test
  public void startUp() throws IOException {
    BaseXServer master = createServer(LOCALHOST, 9999, "-c", "replicate start");
    // TODO start up at different port
    BaseXServer slave = createServer(LOCALHOST, 8888, "-c", "replicate connect localhost:9999");
    
    ClientSession session;
    try {
      // TODO connect to slave, not master
      session = createClient(LOCALHOST, 8888);
      session.setOutputStream(OUT);
      // TODO execute some RPELICATE INFO command, tbd
    } catch(final IOException ex) {
      fail(Util.message(ex));
    } finally {
      slave.stop();
      master.stop();
    }
  }
}
