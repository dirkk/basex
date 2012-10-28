package org.basex.dist.work;

import java.io.*;

import org.basex.dist.*;

/**
 * Represents a work packet, which handles the connection establishment.
 * The given cluster peer tried to connect and a socket channel is already
 * established.
 *
 * @author Dirk Kirsten
 */
public class ConnectionAccept implements WorkPacket {
  /** Connect to the given clusterpeer. */
  private ClusterPeer cp;

  /**
   * Default Constructor.
   *
   * @param cp cluster peer to connect to.
   */
  @SuppressWarnings("hiding")
  public ConnectionAccept(ClusterPeer cp) {
    this.cp = cp;
  }
  
  @Override
  public void execute() throws IOException {
    cp.handleConnect();
  }

}
