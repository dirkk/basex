package org.basex.server.replication;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.cmd.CreateDB;
import org.basex.core.cmd.XQuery;
import org.basex.util.Performance;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

/**
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class DatabaseReplicationTest extends SimpleSandboxTest {
  @Test
  public void replicateSimpleDatabase() throws BaseXException {
    Context ctx1 = createSandbox();
    new CreateDB("databaseTest", "<A><B/></A>").execute(ctx1);
    Context ctx2 = createSandbox();

    ctx1.replication.start(ctx1, new InetSocketAddress("127.0.0.1", 8765), new InetSocketAddress("127.0.0.1", 8766));

    ctx2.replication.start(ctx2, new InetSocketAddress("127.0.0.1", 8767), new InetSocketAddress("127.0.0.1", 8768));
    ctx2.replication.connect(new InetSocketAddress("127.0.0.1", 8765));
    assertEquals("<A>\n  <B/>\n</A>", new XQuery("db:open('databaseTest')").execute(ctx1));
    assertEquals("<A>\n  <B/>\n</A>", new XQuery("db:open('databaseTest')").execute(ctx2));

    ctx1.close();
    ctx2.close();
  }

  @Test
  public void replicateSmallDatabase() throws BaseXException {
    Context ctx1 = createSandbox();
    new CreateDB("databaseTest", "basex-core/src/test/resources/test.xml").execute(ctx1);
    Context ctx2 = createSandbox();

    ctx1.replication.start(ctx1, new InetSocketAddress("127.0.0.1", 8765), new InetSocketAddress("127.0.0.1", 8766));

    ctx2.replication.start(ctx2, new InetSocketAddress("127.0.0.1", 8767), new InetSocketAddress("127.0.0.1", 8768));
    ctx2.replication.connect(new InetSocketAddress("127.0.0.1", 8765));

    assertEquals("text in child", new XQuery("db:open('databaseTest')//childnode/string()").execute(ctx1));
    assertEquals("baz bar blu", new XQuery("db:open('databaseTest')//contextnode/@name/data()").execute(ctx1));
    assertEquals("text in child", new XQuery("db:open('databaseTest')//childnode/string()").execute(ctx2));
    assertEquals("baz bar blu", new XQuery("db:open('databaseTest')//contextnode/@name/data()").execute(ctx2));

    ctx1.close();
    ctx2.close();
  }

  @Test
  public void replicateBiggerDatabase() throws BaseXException {
    Context ctx1 = createSandbox();
    new CreateDB("databaseTest", "basex-core/src/test/resources/factbook.zip").execute(ctx1);
    Context ctx2 = createSandbox();

    ctx1.replication.start(ctx1, new InetSocketAddress("127.0.0.1", 8765), new InetSocketAddress("127.0.0.1", 8766));

    ctx2.replication.start(ctx2, new InetSocketAddress("127.0.0.1", 8767), new InetSocketAddress("127.0.0.1", 8768));
    ctx2.replication.connect(new InetSocketAddress("127.0.0.1", 8765));

    assertEquals(
      "28820672",
      new XQuery("db:open('databaseTest')//country[@name = 'Canada']/@population/data()").execute(ctx1)
    );
    Performance.sleep(2000);
    assertEquals(
      "28820672",
      new XQuery("db:open('databaseTest')//country[@name = 'Canada']/@population/data()").execute(ctx2)
    );

    ctx1.close();
    ctx2.close();
  }
}
