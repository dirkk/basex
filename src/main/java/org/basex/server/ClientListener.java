package org.basex.server;

import java.io.*;
import java.net.*;

import org.basex.core.*;

/**
 * Server-side client session in the client-server architecture.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Andreas Weiler
 * @author Christian Gruen
 */
public final class ClientListener {
  /** Database context. */
  private final Context context;
  /** Socket address. */
  private final InetSocketAddress addr;

  /**
   * Constructor.
   * @param ctx database context
   */
  public ClientListener(final Context ctx, final InetSocketAddress a) {
    context = ctx;
    addr = a;
  }
  /**
   * Returns the context of this session.
   * @return user reference
   */
  public Context context() {
    return context;
  }
  

  /**
   * Exits the session.
   */
  public synchronized void quit() {
    
  }
  
  /**
   * Returns the host and port of a client.
   * @return string representation
   */
  public String address() {
    return addr.getAddress().getHostAddress() + ':' + addr.getPort();
  }
  
  /**
   * Sends a notification to the client.
   * @param name event name
   * @param msg event message
   * @throws IOException I/O exception
   */
  public synchronized void notify(final byte[] name, final byte[] msg)
      throws IOException {
  }
}
