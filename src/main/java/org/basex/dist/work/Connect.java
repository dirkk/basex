package org.basex.dist.work;

import java.io.*;

import org.basex.dist.*;

/**
 * Represents a work packet, which connects to a given peer within the
 * distributed BaseX cluster.
 *
 * @author Dirk Kirsten
 */
public class Connect implements WorkPacket {
  /** Connect to the given clusterpeer. */
  private ClusterPeer cp;

  /**
   * Default Constructor.
   *
   * @param cp cluster peer to connect to.
   */
  @SuppressWarnings("hiding")
  public Connect(ClusterPeer cp) {
    this.cp = cp;
  }
  
  @Override
  public void execute() throws IOException {
    cp.connect();
  }

}
