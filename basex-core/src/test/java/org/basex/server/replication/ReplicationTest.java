package org.basex.server.replication;

import org.basex.BaseXMember;
import org.basex.BaseXServer;
import org.basex.SandboxTest;
import org.basex.core.BaseXException;
import org.basex.core.Command;
import org.basex.core.GlobalOptions;
import org.basex.core.cmd.*;
import org.basex.io.IOFile;
import org.basex.io.out.ArrayOutput;
import org.basex.server.Session;
import org.basex.util.Prop;
import org.basex.util.Util;
import org.basex.util.list.StringList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import static org.basex.core.Text.DB_NOT_FOUND_X;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class tests the basic replication infrastructure.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ReplicationTest extends SandboxTest {
  /** Raw output method. */
  private static final String RAW = "declare option output:indent 'no';";
  /** Output stream. */
  ArrayOutput out;
  /** Master server and session. */
  private ReplicaSetNode master;
  /** Slave references. */
  private List<ReplicaSetSlave> slaves = new LinkedList<ReplicaSetSlave>();
  /** Master host address. */
  private final static String HOST = "localhost";
  private final static int PORT = 9999;
  /** Test database name. */
  private final static String DB = "junit";

  /** Stops a session. 
   * @throws IOException I/O exception
   */
  @After
  public final void stop() throws IOException {
    if(cleanup) {
      master.execute(new DropDB(NAME));
    }
    
    master.stop();
    stopAllSlaves();
  }

  /** Starts a session. 
   * @throws IOException I/O exception
   */
  @Before
  public void start() throws IOException {
    master = new ReplicaSetMaster();
    // start one connected slave
    startSlaves(1);
  }
  
  /**
   * Starts a given number of slave nodes within the replica set.
   * 
   * @param n number of slaves to start
   * @throws IOException I/O exception
   */
  private void startSlaves(final int n) throws IOException {
    for (int i = 0; i < n; ++i) {
      ReplicaSetSlave r = new ReplicaSetSlave(12345 + (i * 2));
      slaves.add(r);
    }
  }
  
  /**
   * Stops all slaves currently running.
   * @throws IOException I/O exception
   */
  private void stopAllSlaves() throws IOException {
    for (ReplicaSetNode r : slaves) {
      r.stop();
    }
  }

  /**
   * The master server already has some data and sends it to a newly connected slave.
   *
   * @throws IOException I/O exception
   * @throws InterruptedException interrupt
   */
  @Test
  public final void transferAllDatabases() throws IOException, InterruptedException {
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));
    master.execute(new Add("testAdd.xml", "<test><ADD/></test>"));

    // start a new slave
    ReplicaSetSlave r = new ReplicaSetSlave(23456);
    r.startSession();

    // check if transfer was successful
    Thread.sleep(500);
    String res = r.execute(new XQuery(RAW + "doc('" + DB + "/testAdd.xml')"));
    assertEqual("<test><ADD/></test>",
            res);

    // stop slave
    r.stop();
  }
  
  /**
   * Execute an ADD document command on the master and checks if the slaves got the
   * update.
   * 
   * @throws IOException I/O exception
   * @throws InterruptedException interrupt
   */
  @Test
  public final void add() throws IOException, InterruptedException {
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));
    master.execute(new Add("testAdd.xml", "<test><ADD/></test>"));
    
    Thread.sleep(200);
    String res = slaves.get(0).execute(new XQuery(RAW + "doc('" + DB + "/testAdd.xml')"));
    assertEqual("<test><ADD/></test>",
        res);
    
    // drop the temporary database
    master.execute(new DropDB(DB));
  }
  
  /**
   * Execute a CREATE DB command on the master and checks if the slaves got the
   * update.
   * 
   * @throws IOException I/O exception
   * @throws InterruptedException interrupt
   */
  @Test
  public final void createDb() throws IOException, InterruptedException {
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));

    Thread.sleep(200);
    slaves.get(0).execute(new Open(DB));
    
    // drop the temporary database
    master.execute(new DropDB(DB));
  }
  
  /**
   * Execute an INSERT INTO on the master and checks if the slaves got the
   * update.
   * 
   * @throws IOException I/O exception
   * @throws InterruptedException interrupt
   */
  @Test
  public final void insertInto() throws IOException, InterruptedException {
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));
    master.execute(new Add("test.xml", "<test></test>"));
    
    master.execute(new XQuery("insert node <test2/> into //test"));

    // it is eventually consistent, so wait a moment
    Thread.sleep(200);
    assertEqual("<test><test2/></test>",
        slaves.get(0).execute(new XQuery(RAW + "doc('" + DB + "/test.xml')")));
    
    // drop the temporary database
    master.execute(new DropDB(DB));
  }
  
  /**
   * Executes an INSERT INTO multiple times on the master and checks if the
   * slaves got the update.
   * 
   * @throws IOException I/O exception
   * @throws InterruptedException interrupt
   */
  @Test
  public final void insertIntoMultiple() throws IOException, InterruptedException {
    final int TRIES = 1000;
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));
    master.execute(new Add("test.xml", "<test></test>"));
    
    for (int i = 0; i < TRIES; ++i)
      master.execute(new XQuery("insert node <test" + i + "/> into //test"));
    
    String expected = "<test>";
    for (int i = 0; i < TRIES; ++i)
      expected += "<test" + i + "/>";
    expected += "</test>";
    
    // it is eventually consistent, so wait a moment
    Thread.sleep(1000);
    assertEqual(expected,
        slaves.get(0).execute(new XQuery(RAW + "doc('" + DB + "/test.xml')")));
    
    // drop the temporary database
    master.execute(new DropDB(DB));
  }
  
  /**
   * Execute an INSERT INTO FIRST on the master and checks if the slaves got the
   * update.
   * 
   * @throws IOException I/O exception
   */
  @Test
  public final void insertIntoFirst() throws IOException, InterruptedException {
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));
    master.execute(new Add("test.xml", "<test><test3/></test>"));
    
    master.execute(new XQuery("insert node <test2/> as first into //test"));

    Thread.sleep(200);
    assertEqual("<test><test2/><test3/></test>",
        slaves.get(0).execute(new XQuery(RAW + "doc('" + DB + "/test.xml')")));
    
    // drop the temporary database
    master.execute(new DropDB(DB));
  }

  /**
   * Drop a database.
   *
   * @throws IOException I/O exception
   */
  @Test
  public final void dropDatabase() throws IOException, InterruptedException {
    // Create a database and add a simple document
    master.execute(new CreateDB(DB));
    master.execute(new Add("test.xml", "<test><test3/></test>"));

    Thread.sleep(200);
    assertEqual("<test><test3/></test>",
            slaves.get(0).execute(new XQuery(RAW + "doc('" + DB + "/test.xml')")));

    // drop the database
    master.execute(new DropDB(DB));
    Thread.sleep(200);
    try  {
      slaves.get(0).execute(new Open(DB));
      fail(new BaseXException(DB_NOT_FOUND_X, DB).getMessage());
    } catch (BaseXException e) {
      assertEquals(e.getMessage(), new BaseXException(DB_NOT_FOUND_X, DB).getMessage());
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
  
  
  /**
   * Master instance of a replica set and associated client session.
   *
   * @author BaseX Team 2005-12, BSD License
   * @author Dirk Kirsten
   */
  private class ReplicaSetMaster extends ReplicaSetNode {
    /**
     * Default constructor.
     * 
     * @throws IOException I/O exception
     */
    public ReplicaSetMaster() throws IOException {
      super();

      final int port = PORT;

      try {
        System.setOut(NULL);
        final StringList sl = new StringList().add("-z").add("-a" + port + 1).add("-p" + port).add("-e" + (port -1));
        final BaseXServer member = new BaseXServer(sl.toArray());
        member.context.globalopts.set(GlobalOptions.DBPATH, new IOFile(Prop.TMP, "sandbox-master").path());
      } finally {
        System.setOut(OUT);
      }
      startSession();
      startReplication(PORT + 1);
    }

    @Override
    public void stop() throws IOException {
      member.context.replication.stop();

      super.stop();
    }
    
    @Override
    public void startSession() {
      try {
        session = createClient();
        session.setOutputStream(out);
      } catch(final IOException ex) {
        fail(Util.message(ex));
      }
    }
  }
  
  /**
   * Slave instance of a replica set and associated client session.
   *
   * @author BaseX Team 2005-12, BSD License
   * @author Dirk Kirsten
   */
  private class ReplicaSetSlave extends ReplicaSetNode {
    /** Port. */
    final int port;
    
    /**
     * Default constructor.
     * 
     * @param p number to use, event port will be port + 1.
     * @throws IOException I/O exception
     */
    public ReplicaSetSlave(final int p) throws IOException {
      super();
      
      port = p;
      final int port = PORT;

      try {
        System.setOut(NULL);
        final StringList sl = new StringList().add("-z").add("-a" + port + 1).add("-p" + port).add("-e" + (port -1));
        member = new BaseXMember(sl.toArray());
        member.context.globalopts.set(GlobalOptions.DBPATH, new IOFile(Prop.TMP, "sandbox-master").path());
      } finally {
        System.setOut(OUT);
      }
      startSession();
      startReplication(p + 1);
    }
    
    @Override
    public void startSession() {
      try {
        session = createClient("localhost", port);
        session.setOutputStream(out);
      } catch(final IOException ex) {
        fail(Util.message(ex));
      }
    }
  }
  
  /**
   * Either a master or a slave instance.
   * 
   * @author BaseX Team 2005-12, BSD License
   * @author Dirk Kirsten
   */
  private abstract class ReplicaSetNode {
    /** Server reference. */
    protected BaseXMember member;
    /** Session reference. */
    protected Session session;
    
    /**
     * Default constructor.
     */
    public ReplicaSetNode() {
    }
    
    /**
     * Executes a command using the client session.
     *
     * @param cmd command to execute
     * @return command result
     * @throws IOException I/O exception
     */
    public String execute(final Command cmd) throws IOException {
      return session.execute(cmd);
    }
    
    /**
     * Stops the server instance.
     *
     * @throws IOException I/O exception
     */
    public void stop() throws IOException {
      try {
        session.close();
      } catch(final IOException ex) {
        fail(Util.message(ex));
      }

      member.stop();
    }
    
    /**
     * Starts the client session.
     */
    public abstract void startSession();

    /**
     * Start the replication of this node as master instance.
     */
    public void startReplication(final int port) throws IOException {
      member.context.replication.start(member.context, new InetSocketAddress(port), new InetSocketAddress(port + 1));
    }
  }
}
