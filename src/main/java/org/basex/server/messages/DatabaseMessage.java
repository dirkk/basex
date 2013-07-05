package org.basex.server.messages;

import java.io.*;

/**
 * Contains a whole database. Send for replication.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class DatabaseMessage {
  /** Main database file. */
  private final File file;
  
  /**
   * Constructor.
   * @param f file instance
   */
  public DatabaseMessage(final File f) {
    file = f;
  }
  
  public File getFile() {
    return file;
  }
}
