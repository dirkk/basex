package org.basex.test.server.replication;

import org.basex.core.Context;
import org.basex.core.Replication;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.basex.server.replication.ReplicationExceptions.ReplicationAlreadyRunningException;

/**
 * Testing the primary election handling of a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ConnectionTest {
  @BeforeClass
  public static void setup() {
  }

  @Before
  public void startup() {
  }

  @After
  public void teardown() {
  }

  @Test
  public void startConnection() throws Exception, ReplicationAlreadyRunningException {
    final int MAX_TRIES = 100;

    Replication repl1 = Replication.getInstance(new Context());
    Replication repl2 = Replication.getInstance(new Context());
    repl1.start("127.0.0.1", 8765);

    repl2.connect("127.0.0.1", 8762, "127.0.0.1", 8765);

    boolean infoAvailable = false;
    int tries = 0;
    String info = "";
    while (!infoAvailable && tries < MAX_TRIES) {
      info = repl1.info();
      if (info.equals("No information available")) {
        ++tries;
        Thread.sleep(100);
      } else {
        infoAvailable = true;
      }
    }


    assert(info.startsWith("State: PRIMARY"));

    Thread.sleep(1000);
    String repl2Info = repl2.info();
    assert(repl2Info.startsWith("State: SECONDARY"));
  }

  @Test
  public void noConnectAfterStart() throws Exception, ReplicationAlreadyRunningException {
    Replication repl1 = Replication.getInstance(new Context());
    repl1.start("127.0.0.1", 8765);

    ExpectedException.none().expect(ReplicationAlreadyRunningException.class);
    repl1.connect("127.0.0.1", 8762, "127.0.0.1", 8765);
  }


  @Test
  public void startConnectionThreeMembers() throws Exception, ReplicationAlreadyRunningException {
    final int MAX_TRIES = 100;

    Replication repl1 = Replication.getInstance(new Context());
    Replication repl2 = Replication.getInstance(new Context());
    Replication repl3 = Replication.getInstance(new Context());
    repl1.start("127.0.0.1", 8765);

    repl2.connect("127.0.0.1", 8762, "127.0.0.1", 8765);
    repl3.connect("127.0.0.1", 8760, "127.0.0.1", 8765);

    boolean infoAvailable = false;
    int tries = 0;
    String info = "";
    while (!infoAvailable && tries < MAX_TRIES) {
      info = repl1.info();
      if (info.equals("No information available")) {
        ++tries;
        Thread.sleep(100);
      } else {
        infoAvailable = true;
      }
    }

    assert(info.startsWith("State: "));
  }

  @Test
  public void startConnectionTwentyMembers() throws Exception, ReplicationAlreadyRunningException {
    // TODO does not work, only three members connected
    final int RUNS = 20;
    final int MAX_TRIES = 100;

    Replication repl1 = Replication.getInstance(new Context());
    repl1.start("127.0.0.1", 8765);

    for (int i = 0; i < RUNS; ++i) {
      Replication.getInstance(new Context()).connect("127.0.0.1", 8766 + i, "127.0.0.1", 8765);
    }

    Thread.sleep(2000);

    boolean infoAvailable = false;
    int tries = 0;
    String info = "";
    while (!infoAvailable && tries < MAX_TRIES) {
      info = repl1.info();
      if (info.equals("No information available")) {
        ++tries;
        Thread.sleep(300);
      } else {
        infoAvailable = true;
      }
    }

    assert(info.startsWith("State: "));
  }


}
