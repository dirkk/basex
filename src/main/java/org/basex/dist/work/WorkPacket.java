package org.basex.dist.work;

import java.io.*;

/**
 * Represents a work packet in a working queue to be executed by the
 * NetworkPeer class. This packages are placed in the workQueue there
 * and are handled by the separate thread.
 *
 * @author Dirk Kirsten
 */
public interface WorkPacket {
  /**
   * Execute the work.
   * @throws IOException I/O exception.
   */
  public void execute() throws IOException;
}
