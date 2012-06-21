package org.basex.test.dist;

import static org.junit.Assert.*;

import java.io.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.io.*;
import org.basex.util.*;
import org.junit.*;

/**
 * Tests a simple query for the distributed version of BaseX.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class QueryTest {
  /** Number of super-peers. */
  private static final int NUM_SUPERPEERS = 1;
  /** Number of peers per super-peer. */
  private static final int NUM_PEERS = 4;
  /** starting port. */
  private static int localPort = 22000;
  /** Threads simulating the different peers within the network. */
  private static Peer[] baseXThread;

  /**
   * Start peers and build the overlay network topology.
   * @throws Exception exception
   */
  @BeforeClass
  public static void createNetwork() throws Exception {
    int clusterPort = 0;
    baseXThread = new Peer[(NUM_PEERS + 1) * NUM_SUPERPEERS];
    int iT = 0;

    for (int i = 0; i < NUM_SUPERPEERS; ++i) {
      if (i == 0) {
        // start first peer
        baseXThread[iT] = new Peer("localhost", localPort);
        ++iT;
      } else {
        // create super-peer for this cluster
        baseXThread[iT] = new Peer("localhost", localPort, "localhost", clusterPort,
            true);
        ++iT;
      }
      clusterPort = localPort;
      localPort += 10;

      for (int j = 0; j < NUM_PEERS; ++j) {
        try {
          baseXThread[iT] = new Peer("localhost", localPort, "localhost", clusterPort);
        } catch (BaseXException e) {
          System.err.println("Exception");
          System.err.println(e.getMessage());
          e.printStackTrace();
          throw new Exception();
        }
        ++iT;
        localPort += 10;
      }
    }
  }

  /**
   * Destroys the threads.
   */
  @After
  public void cleanup() {
    for (int i = 0; i < baseXThread.length; ++i) {
      baseXThread[i].close();
    }
  }

  /**
   * Executes the query on one of the peers within the network.
   * @throws InterruptedException Thread interrupted.
   */
  @Test
  public void executeQuery() throws InterruptedException {
    baseXThread[1].start();
    baseXThread[1].join();
  }

  /**
   * Executes the query on all of the peers within the network in
   * sequential order.
   * @throws InterruptedException Thread interrupted.
   */
  @Test
  public void executeQueryAll() throws InterruptedException {
    for (int i = 0; i < baseXThread.length; ++i) {
      baseXThread[i].start();
      baseXThread[i].join();
    }
  }

  /** Network peer. */
  private static class Peer extends Thread {
    /** Test name. */
    public final String name;
    /** BaseX instance. */
    public Context ctx;
    /** number of instances. */
    public static int number = 1;

    /**
     * Creates the sandbox.
     */
    public void createSandbox() {
      final IOFile sb = sandbox();
      sb.delete();
      assertTrue("Sandbox could not be created.", sb.md());
      ctx = new Context();
      initContext(ctx);
    }

    /**
     * Peer constructor.
     * @param host host name.
     * @param port port number.
     * @throws IOException I/O exception while establishing the session
     */
    public Peer(final String host, final int port) throws IOException {
      synchronized(this) {
        name = Util.name(QueryTest.class) + String.valueOf(number);
        ++number;
      }
      createSandbox();
      ctx.mprop.set(MainProp.DBPATH, sandbox().path());
      new Distribute(host, String.valueOf(port)).execute(ctx);
    }

    /**
     * Peer constructor.
     * @param host host name.
     * @param port port number.
     * @param cHost host name to connect to.
     * @param cPort port number to connect to.
     * @throws IOException I/O exception while establishing the session
     */
    public Peer(final String host, final int port, final String cHost, final int cPort)
        throws IOException {
      synchronized(this) {
        name = Util.name(QueryTest.class) + String.valueOf(number);
        ++number;
      }
      createSandbox();
      ctx.mprop.set(MainProp.DBPATH, sandbox().path());
      new Distribute(host, String.valueOf(port), cHost, String.valueOf(cPort), false).execute(ctx);
    }

    /**
     * Peer constructor.
     * @param host host name.
     * @param port port number.
     * @param cHost host name to connect to.
     * @param cPort port number to connect to.
     * @param superpeer This peer should be a superpeer.
     * @throws IOException I/O exception while establishing the session
     */
    public Peer(final String host, final int port, final String cHost, final int cPort,
        @SuppressWarnings("unused") final boolean superpeer) throws IOException {
      synchronized(this) {
        name = Util.name(QueryTest.class) + String.valueOf(number);
        ++number;
      }
      createSandbox();
      ctx.mprop.set(MainProp.DBPATH, sandbox().path());
      new Distribute(host, String.valueOf(port), cHost, String.valueOf(cPort), true)
        .execute(ctx);
    }

    @Override
    public void run() {
      try {
        String result = new DiXQuery("<li>1</li>").execute(ctx);
        System.err.println(result);
      } catch(final Exception ex) {
        Util.stack(ex);
      }
    }

    /**
     * Initializes the specified context.
     * @param context context
     */
    protected void initContext(final Context context) {
      final IOFile sb = sandbox();
      context.mprop.set(MainProp.DBPATH, sb.path() + "/data");
      context.mprop.set(MainProp.HTTPPATH, sb.path() + "/http");
      context.mprop.set(MainProp.REPOPATH, sb.path() + "/repo");
    }

    /**
     * Removes test databases and closes the database context.
     */
    public void close() {
      ctx.close();
      assertTrue("Sandbox could not be deleted.", sandbox().delete());
    }

    /**
     * Returns the sandbox database path.
     * @return database path
     */
    public IOFile sandbox() {
      return new IOFile(Prop.TMP, name);
    }
  }
}
