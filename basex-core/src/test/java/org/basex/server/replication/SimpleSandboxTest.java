package org.basex.server.replication;

import org.basex.core.Context;
import org.basex.core.GlobalOptions;
import org.basex.io.IOFile;
import org.basex.util.Prop;

import static org.junit.Assert.assertTrue;

/**
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class SimpleSandboxTest {
  /** Sandbox counter. */
  private static int SANDBOX = 0;

  protected Context createSandbox() {
    final IOFile sb =  new IOFile(Prop.TMP, "Sandbox" + ++SANDBOX);
    sb.delete();
    assertTrue("Sandbox could not be created.", sb.md());
    Context ctx = new Context();
    ctx.globalopts.set(GlobalOptions.DBPATH, sb.path() + "/data");
    ctx.globalopts.set(GlobalOptions.WEBPATH, sb.path() + "/webapp");
    ctx.globalopts.set(GlobalOptions.RESTXQPATH, sb.path() + "/webapp");
    ctx.globalopts.set(GlobalOptions.REPOPATH, sb.path() + "/repo");

    return ctx;
  }
}
