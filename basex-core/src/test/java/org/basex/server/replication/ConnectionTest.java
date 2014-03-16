package org.basex.server.replication;

import org.basex.core.Context;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Testing the primary election handling of a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ConnectionTest extends SimpleSandboxTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void startConnection() {
    Context ctx1 = createSandbox();
    Context ctx2 = createSandbox();

    assert(ctx1.replication.start(ctx1, new InetSocketAddress("127.0.0.1", 8765), new InetSocketAddress("127.0.0.1", 8766)));

    assert(ctx2.replication.start(ctx2, new InetSocketAddress("127.0.0.1", 8767), new InetSocketAddress("127.0.0.1", 8768)));
    assert(ctx2.replication.connect(new InetSocketAddress("127.0.0.1", 8765)));

    ctx1.close();
    ctx2.close();
  }

  @Test
  public void startConnectionThreeMembers() {
    Context ctx1 = createSandbox();
    Context ctx2 = createSandbox();
    Context ctx3 = createSandbox();

    assert(ctx1.replication.start(ctx1, new InetSocketAddress("127.0.0.1", 8765), new InetSocketAddress("127.0.0.1", 8766)));

    assert(ctx2.replication.start(ctx2, new InetSocketAddress("127.0.0.1", 8767), new InetSocketAddress("127.0.0.1", 8768)));
    assert(ctx2.replication.connect(new InetSocketAddress("127.0.0.1", 8765)));

    assert(ctx3.replication.start(ctx3, new InetSocketAddress("127.0.0.1", 8769), new InetSocketAddress("127.0.0.1", 8770)));
    assert(ctx3.replication.connect(new InetSocketAddress("127.0.0.1", 8765)));

    String info = ctx1.replication.info();
    assert(info.startsWith("State: RUNNING"));
    assert(info.contains("Number of secondaries: 2"));

    ctx1.close();
    ctx2.close();
    ctx3.close();
  }

  @Test
  public void startConnectionTenMembers() {
    Context ctxMain = createSandbox();
    assert(ctxMain.replication.start(ctxMain, new InetSocketAddress("127.0.0.1", 8765), new InetSocketAddress("127.0.0.1", 8766)));


    List<Context> ctxs = new ArrayList<Context>();
    for (int i = 0; i < 10; ++i) {
      ctxs.add(createSandbox());
    }

    for (int i = 0; i < 10; ++i) {
      final Context c = ctxs.get(i);
      assert(c.replication.start(c, new InetSocketAddress("127.0.0.1", 8770 + i * 2), new InetSocketAddress("127.0.0.1", 8771 + i * 2)));
      assert(c.replication.connect(new InetSocketAddress("127.0.0.1", 8765)));
    }

    String info = ctxMain.replication.info();
    assert(info.startsWith("State: RUNNING"));
    assert(info.contains("Number of secondaries: 10"));

    ctxMain.close();
    for (Context c : ctxs) c.close();
  }
}
