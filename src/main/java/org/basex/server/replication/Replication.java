package org.basex.server.replication;

import java.net.*;

import org.basex.server.messages.*;

import akka.actor.*;

/**
 * To be implemented by the client/server infrastructure to
 * publish changes to connected slaves.
 * Gets called by data manipulation commands and replicates the
 * whole database.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class Replication {
  /** Server actor. */
  private final ActorRef node;
  /** Replication actor. */
  private ActorRef repl = null;
  
  /**
   * Default constructor.
   * @param n server node
   */
  public Replication(final ActorRef n) {
    node = n;
  }
  
  /**
   * This object was modified, so push the changes to
   * the connected slaves.
   * @param o object to replicate
   */
  public void replicate(final Object o) {
    if (repl == null)
      return;
    
    repl.tell(o, null);
  }
  
  /**
   * Start this node as master.
   * @return success
   */
  public boolean start() {
    node.tell(new ServerCommandMessage(InternalServerCmd.STARTMASTER), null);
    return true;
  }
  
  /**
   * Connect to a master.
   * @param master master address
   * @return success
   */
  public boolean connect(final InetSocketAddress master) {
    node.tell(new ServerCommandMessage(InternalServerCmd.CONNECTMASTER, master), null);
    return true;
  }
}
