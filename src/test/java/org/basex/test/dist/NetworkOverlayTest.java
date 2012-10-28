package org.basex.test.dist;

import static org.junit.Assert.*;

import java.io.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.io.*;
import org.basex.util.*;
import org.junit.*;

/**
 * Test the overlay P2P network infrastructure for the distributed BaseX
 * version.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class NetworkOverlayTest {
  /** Number of peers to create. */
  private static final int PEERS = 2;
  /** starting port, will be increased by 10 for every new peer. */
  private static final int START_PORT = 22000;
  /** port to be used for the next peer as server port. */
  private static int localPort = START_PORT;

  /**
   * Start peers and build the overlay network topology.
   * @throws Exception exception
   */
  @Test
  public void createNetwork() throws Exception {
    Peer[] t = new Peer[PEERS];

    // start first peer, connect to nothing
    t[0] = new Peer("localhost", localPort);
    //t[0].start();
    //t[0].join();
    
    // start all other peers, connect to first peer
    for (int i = 1; i < PEERS; ++i) {
      localPort += 10;
      Thread.sleep(1000);
      
      t[i] = new Peer("localhost", localPort, "localhost", START_PORT);
      //t[i].start();
      //t[i++].join();
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
      new Distribute(host, String.valueOf(port), cHost, String.valueOf(cPort)).execute(ctx);
    }

    @Override
    public void run() {
      System.err.println("Thread NetworkOverlayTest.Peer runs.");
      try {
        sleep(10000);
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
