package org.basex.test.dist;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.io.*;
import org.basex.util.*;
import org.junit.*;

/**
 * Test the overlay P2P network infrastructure for the distributed BaseX version.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class NetworkOverlayTest {
  /** Number of super-peers. */
  private static final int NUM_SUPERPEERS = 1;
  /** Minimum number of peers per super-peer. */
  private static final int MIN_PEERS = 3;
  /** Maximum number of peers per super-peer. */
  private static final int MAX_PEERS = 8;
  /** starting port. */
  private static int localPort = 22000;

  /**
   * Start peers and build the overlay network topology.
   * @throws Exception exception
   */
  @SuppressWarnings("unused")
  @Test
  public void createNetwork() throws Exception {
    Random generator = new Random();
    int clusterPort = 0;
    Peer[] t = new Peer[(MAX_PEERS + 1) * NUM_SUPERPEERS];
    int iT = 0;

    for(int i = 0; i < NUM_SUPERPEERS; ++i) {
      if(i == 0) {
        // start first peer
        t[iT] = new Peer("localhost", localPort);
        t[iT].start();
        t[iT].join();
        ++iT;
      } else {
        // create super-peer for this cluster
        t[iT] = new Peer("localhost", localPort, "localhost", clusterPort, true);
        t[iT].start();
        t[iT].join();
        ++iT;
      }
      clusterPort = localPort;
      localPort += 10;
      int max;
      if (MAX_PEERS == MIN_PEERS) {
        max = MIN_PEERS;
      } else {
        max = generator.nextInt(MAX_PEERS + 1 - MIN_PEERS) + MIN_PEERS;
      }

      for(int j = 0; j < max; ++j) {
        try {
          t[iT] = new Peer("localhost", localPort, "localhost", clusterPort);
        } catch (BaseXException e) {
          System.err.println("Exception");
          System.err.println(e.getMessage());
          e.printStackTrace();
          throw new Exception();
        }
        t[iT].start();
        t[iT].join();
        ++iT;
        localPort += 10;
      }
    }
  }

  /** Network peer. */
  private static class Peer extends Thread {
    /** Test name. */
    public final String name;
    /** BaseX instance. */
    public Context ctx;
    /** Clean up files. */
    public boolean cleanup = true;
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
        name = Util.name(NetworkOverlayTest.class) + String.valueOf(number);
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
        name = Util.name(NetworkOverlayTest.class) + String.valueOf(number);
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
        final boolean superpeer) throws IOException {
      synchronized(this) {
        name = Util.name(NetworkOverlayTest.class) + String.valueOf(number);
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
        synchronized(this) {
          wait(1000);
        }
        String output = new ShowNetwork().execute(ctx);
        if (output.contains("State: DISCONNECTED") || output.contains("State: PENDING")
            ) {
          throw new BaseXException("A peer did not successfully connect",
              new Exception());
        }
        close();
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
      if(cleanup) {
        ctx.close();
        assertTrue("Sandbox could not be deleted.", sandbox().delete());
      }
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
