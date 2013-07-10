package org.basex.test.server;

import static org.junit.Assert.*;
import static org.basex.core.Text.*;

import java.io.*;

import org.basex.*;
import org.basex.io.out.*;
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
  /** Output stream. */
  private static ArrayOutput out;
  /** Master node. */
  private static BaseXServer master;
  /** Slave 1 node. */
  private static BaseXServer slave1;
  
  /**
   * Executed before JUnit tests.
   * @throws IOException I/O exception
   */
  @BeforeClass
  public static void pre() throws IOException {
    out = new ArrayOutput();
    master = createServer(LOCALHOST, 9999, "-r", "localhost:10999", "-c", "replicate start");
    slave1 = createServer(LOCALHOST, 8888, "-r", "localhost:8899", "-c", "replicate connect localhost:10999");
  }
  
  /**
   * Executed after all JUnit test were executed.
   */
  @AfterClass
  public static void post() {
    slave1.stop();
    master.stop();
  }

  /**
   * Test the startup of one master and one connected
   * slave node.
   */
  @Test
  public void startUp() {
    ClientSession session;
    try {
      session = createClient(LOCALHOST, 9999);
      session.setOutputStream(out);
      session.execute("INFO REPLICATION");
    } catch(final IOException ex) {
      fail(Util.message(ex));
    }
  }
  
  /**
   * Test the 'info replication' command for a master and slave.
   */
  @Test
  public void info() {
    ClientSession slaveSession, masterSession;
    try {
      masterSession = createClient(LOCALHOST, 9999);
      masterSession.setOutputStream(out);
      assertEqual(
          "General Information Activated: true Master/Slave: Master",
          masterSession.execute("info replication")
          );
      
      slaveSession = createClient(LOCALHOST, 8888);
      slaveSession.setOutputStream(out);
      assertEqual(
          "General Information Activated: true Master/Slave: Slave Master: localhost:10999",
          slaveSession.execute("info replication")
          );
    } catch(final IOException ex) {
      fail(Util.message(ex));
    }
  }

  /**
   * Checks if the most recent output equals the specified string.
   * @param exp expected string
   * @param ret string returned from the client API
   */
  protected final void assertEqual(final Object exp, final Object ret) {
    final String result = (out != null ? out : ret).toString();
    if(out != null) out.reset();
    assertEquals(exp.toString(), result.replaceAll("\\r|\\n", ""));
  }
}
