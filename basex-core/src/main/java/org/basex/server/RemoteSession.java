package org.basex.server;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public interface RemoteSession {
  public String exec(final ServerCmd cmd, final String arg, final OutputStream os) throws IOException;
  public void execCache(final ServerCmd cmd, final String arg, final Query q) throws IOException;
  public void send(final String s) throws IOException;
}
