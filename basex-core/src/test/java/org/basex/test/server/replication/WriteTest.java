package org.basex.test.server.replication;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.GlobalOptions;
import org.basex.core.Replication;
import org.basex.core.cmd.*;
import org.basex.io.IOFile;
import org.basex.server.replication.ReplicationActor;
import org.basex.util.Performance;
import org.basex.util.Prop;
import org.basex.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.basex.server.replication.ReplicationExceptions.ReplicationAlreadyRunningException;
import static org.junit.Assert.*;

/**
 * Testing the primary election handling of a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class WriteTest {
  private Context ctx1;
  private Context ctx2;
  private List<Replication> repls;

  @BeforeClass
  public static void setup() {
  }

  @Before
  public void startup() throws Exception, ReplicationAlreadyRunningException {
    ctx1 = createSandbox(1);
    ctx2 = createSandbox(2);

    repls = new LinkedList<Replication>();
    repls.add(Replication.getInstance(ctx1));
    repls.add(Replication.getInstance(ctx2));
    startConnection(repls);
  }

  @After
  public void teardown() throws InterruptedException {
    ctx1.close();
    ctx2.close();

    for (Replication r : repls) {
      r.stop();
    }

    Thread.sleep(1000);
  }

  private Context createSandbox(final int number) {
    final IOFile sb =  new IOFile(Prop.TMP, Util.className(WriteTest.class) + number);
    sb.delete();
    assertTrue("Sandbox could not be created.", sb.md());
    Context ctx = new Context();
    ctx.globalopts.set(GlobalOptions.DBPATH, sb.path() + "/data");
    ctx.globalopts.set(GlobalOptions.WEBPATH, sb.path() + "/webapp");
    ctx.globalopts.set(GlobalOptions.RESTXQPATH, sb.path() + "/webapp");
    ctx.globalopts.set(GlobalOptions.REPOPATH, sb.path() + "/repo");

    return ctx;
  }

  private void startConnection(List<Replication> repls) throws Exception, ReplicationAlreadyRunningException {
    final int MAX_TRIES = 100;

    repls.get(0).start("127.0.0.1", 8765);

    for (int i = 1; i < repls.size(); ++i) {
      repls.get(i).connect("127.0.0.1", 8770 + i * 2, "127.0.0.1", 8765);
    }

    for (Replication r : repls) {
      boolean infoAvailable = false;
      int tries = 0;
      String info;
      while (!infoAvailable && tries < MAX_TRIES) {
        info = r.info();
        if (info.equals("No information available") || info.contains("UNCONNECTED")) {
          ++tries;
          Thread.sleep(100);
        } else {
          infoAvailable = true;
        }
      }


      if (!infoAvailable) throw new Exception("Connection setup failed.");
    }

    System.out.println("Connection setup completed.");
  }

  @Test
  public void add() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new Add("test.xml", "<A />").execute(ctx1);

      Thread.sleep(1000);

      String after = new XQuery("db:open('test')").execute(ctx2);
      System.out.println("After: " + after);
    }
  }

  @Test
  public void create() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);

      Thread.sleep(1000);

      new Open("test").execute(ctx2);
    }
  }

  @Test
  public void alter() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('testNew')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new AlterDB("test", "testNew").execute(ctx1);

      Thread.sleep(1000);

      new Open("testNew").execute(ctx2);
    }
  }

  @Test
  public void drop() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      Thread.sleep(1000);
      new Open("test").execute(ctx2);

      new DropDB("test").execute(ctx1);
      Thread.sleep(1000);

      try {
        new Open("test").execute(ctx2);
        fail("Database should be deleted.");
      } catch (BaseXException ex2) {
        assert(ex2.getMessage().contains("Database 'test' was not found"));
      }
    }
  }

  @Test
  public void copy() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('testNew')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new Add("test.xml", "<A />").execute(ctx1);
      new Copy("test", "testNew").execute(ctx1);

      Thread.sleep(1000);

      assertEquals("<A/>", new XQuery("db:open('testNew')//A").execute(ctx2));
    }
  }

  @Test
  public void delete() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new Add("test.xml", "<A />").execute(ctx1);
      Thread.sleep(1000);

      assertEquals("<A/>", new XQuery("db:open('test')").execute(ctx2));

      new Delete("test/test.xml").execute(ctx1);
      Thread.sleep(1000);

      assertEquals("", new XQuery("db:open('test')").execute(ctx2));
    }
  }

  @Test
  public void rename() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new Add("test.xml", "<A />").execute(ctx1);
      Thread.sleep(1000);

      assertEquals("test/test.xml", new XQuery("db:list('test')").execute(ctx2));

      new Rename("test/test.xml", "test/testNew.xml").execute(ctx1);
      Thread.sleep(1000);

      assertEquals("test/testNew.xml", new XQuery("db:list('test')").execute(ctx2));
    }
  }

  @Test
  public void update() throws Exception, ReplicationAlreadyRunningException {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new Add("test.xml", "<A />").execute(ctx1);
      Thread.sleep(1000);

      assertEquals("<A/>", new XQuery("db:open('test')").execute(ctx2));

      new XQuery("insert node <B/> into //A").execute(ctx1);
      Thread.sleep(1500);

      assertEquals("<A>\n  <B/>\n</A>", new XQuery("db:open('test')").execute(ctx2));
    }
  }

  public static Performance perf = ReplicationActor.perf;


  @Test
  public void performanceTest() throws Exception, ReplicationAlreadyRunningException {
    final int TRIES = 1000;

    for (int j = 0; j < 20; ++j) {
      // normal version
//      final Context ctx3 = createSandbox(3);
//      for (int i = 0; i < TRIES; ++i) {
//        new CreateDB("test").execute(ctx3);
//        new Add("test.xml", "<A />").execute(ctx3);
//
//        long t1 = System.nanoTime();
//        new XQuery("insert node <B/> into //A").execute(ctx3);
//        tNormal += System.nanoTime() - t1;
//
//        new DropDB("test").execute(ctx3);
//      }
//      System.out.println("Normal    : " + p);

      // replicated version
      for (int i = 0; i < TRIES; ++i) {
        new CreateDB("test").execute(ctx1);
        new Add("test.xml", "<A />").execute(ctx1);

        new XQuery("insert node <B/> into //A").execute(ctx1);

        new DropDB(("test")).execute(ctx1);
      }
      System.out.println("Replicated: " + perf);

    }

    Performance.sleep(10000);
  }

}
