package org.basex.server.replication;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.*;
import org.basex.util.Performance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Testing the primary election handling of a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class WriteTest extends SimpleSandboxTest {
  private Context ctx1;
  private Context ctx2;
  private List<Context> repls;

  @Before
  public void startup() throws Exception {
    ctx1 = createSandbox();
    ctx2 = createSandbox();

    repls = new LinkedList<Context>();
    repls.add(ctx1);
    repls.add(ctx2);
    startConnection(repls);
  }

  @After
  public void teardown() throws InterruptedException {
    ctx1.close();
    ctx2.close();
  }

  private void startConnection(List<Context> repls) throws Exception {
    repls.get(0).replication.start(repls.get(0), new InetSocketAddress("127.0.0.1", 8765),
      new InetSocketAddress("127.0.0.1", 8766));

    for (int i = 1; i < repls.size(); ++i) {
      repls.get(0).replication.start(repls.get(i), new InetSocketAddress("127.0.0.1", 8770 + i * 2),
        new InetSocketAddress("127.0.0.1", 8771 + i * 2 ));
      repls.get(i).replication.connect(new InetSocketAddress("127.0.0.1", 8765));
    }

    System.out.println("Connection setup completed.");
  }

  @Test
  public void add() throws Exception {
    try {
      new XQuery("db:open('test')").execute(ctx2);

      fail("Database should not already be present.");
    } catch (BaseXException ex) {
      assert(ex.getMessage().contains("[BXDB0002]"));

      new CreateDB("test").execute(ctx1);
      new Add("test.xml", "<A />").execute(ctx1);

      Thread.sleep(1000);

      assertEquals("<A/>", new XQuery("db:open('test')").execute(ctx2));
    }
  }

  @Test
  public void create() throws Exception {
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
  public void alter() throws Exception {
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
  public void drop() throws Exception {
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
  public void copy() throws Exception {
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
  public void delete() throws Exception {
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
  public void rename() throws Exception {
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
  public void update() throws Exception {
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

  // public static Performance perf = ReplicationActor.perf;

  @Test
  public void addDocumentsConcurrently() throws BaseXException {
    final int TRIES = 100;

    new CreateDB("test").execute(ctx1);

    for (int i = 0; i < TRIES; ++i) {
      new Add("test.xml", "<A />").execute(ctx1);
    }

    Performance.sleep(500);
  }

  @Test
  public void createDatabaseConcurrently() throws BaseXException {
    final int TRIES = 100;

    for (int i = 0; i < TRIES; ++i) {
      new CreateDB("test").execute(ctx1);
    }

    Performance.sleep(500);
  }

  @Test
  public void performanceTest() throws Exception {
    final int TRIES = 100;

/*

    final Context ctx3 = createSandbox();
    for (int i = 0; i < 1000; ++i) {
      new CreateDB("test").execute(ctx3);
      //new Add("test.xml", "<A />").execute(ctx1);

      //new XQuery("insert node <B/> into //A").execute(ctx1);

      //new DropDB(("test")).execute(ctx1);
    }
    */


    for (int j = 0; j < 1; ++j) {
      // normal version

      /*
      new CreateDB("test").execute(ctx3);
      for (int i = 0; i < 10; ++i) {
        new Add("test.xml", "<A />").execute(ctx3);

        //new XQuery("insert node <B/> into //A").execute(ctx3);

        //new DropDB("test").execute(ctx3);
      }
      //System.out.println("Normal    : " + perf);*/


      // replicated version

      for (int i = 0; i < TRIES; ++i) {
        new CreateDB("test").execute(ctx1);
        //new Add("test.xml", "<A />").execute(ctx1);

        //new XQuery("insert node <B/> into //A").execute(ctx1);

        //new DropDB(("test")).execute(ctx1);
      }
      //System.out.println("Replicated: " + perf);

    }

    Performance.sleep(500);
  }

}
