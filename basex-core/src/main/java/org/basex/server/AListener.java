package org.basex.server;

import org.basex.core.Context;

import java.io.IOException;

/**
 * Abstract Listener class.
 */
public interface AListener {
  public abstract long getId();

  public abstract String address();

  public abstract void quit();

  public abstract Context dbCtx();

  public abstract long last();

  public abstract void notify(final byte[] name, final byte[] msg) throws IOException;
}
