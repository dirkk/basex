package org.basex.core.cmd;

import java.io.*;

import org.basex.core.*;

/**
 * Abstract class for replication start and stop commands.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public abstract class AReplication extends Command {
  /**
   * Constructor.
   *
   * @param arg arguments
   */
  protected AReplication(final String... arg) {
    super(Perm.ADMIN, false, arg);
  }

  /**
   * Gets and checks the raw input of an AMQP URI for a server address.
   * Prepends the input with amqp://, if not already present.
   *
   * @param raw user input
   * @return complete amqp uri
   * @throws IOException AMQP URI is not valid
   */
  protected String parseAMQP(final String raw) throws IOException {
    String o = raw.trim();
    if (!o.startsWith("amqp://") && !o.startsWith("amqps://")) {
      o = "amqp://" + o;
    }
    
    /*
     * An AMQP URI has the following format, only the the protocol and
     * the host are required:
     * amqp[s]://username:password@host:port/vhost
     */
    if (!o.matches("amqp[s]?://([A-Za-z0-9_-]+(:[A-Za-z0-9_-]+)?@)?"
        + "([A-Za-z0-9_-]+)(:[0-9]+)?(/[A-Za-z0-9_-]+)?"))
      throw new IOException("This is not a valid AMQP URI.");
    
    return o;
  }
}
