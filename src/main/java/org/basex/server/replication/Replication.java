package org.basex.server.replication;

import java.net.*;
import java.util.*;

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
  /** Replication master publishing actor. */
  private ActorRef pub = null;
  
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
    if (pub == null)
      return;
    
    pub.tell(o, null);
  }
  
  /**
   * Set the publishing actor.
   * @param p publisher
   */
  public void setPublisher(final ActorRef p) {
    pub = p;
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
  
  /**
   * Returns true if this is a master, if it is a slave or no replication
   * is enabled it will return false.
   * @return is master
   */
  public boolean isMaster() {
    return (pub == null) ? false : true;
  }
  
  /**
   * Returns true if this is a slave connected to a master. If it is a
   * master or no replication is enabled it will return false.
   * @return is master
   */
  public boolean isSlave() {
    return (pub != null) ? false : true;
  }
  
  /**
   * Returns a list of addresses of all connected slaves. If this is not a
   * master replicator it will return an empty list.
   * @return list of address of the slaves
   */
  public List<InetSocketAddress> slaveAddresses() {
    // TODO
    return new LinkedList<InetSocketAddress>();
  }
  
  /**
   * Returns the address of the commanding master node. If this is not a slave
   * node it will return null;
   * @return address of the master node or null
   */
  public String masterAddress() {
    if (pub == null)
      return null;
    
    String h = pub.path().address().host().get();
    Integer p = (Integer) pub.path().address().port().get();
    return h + ":" + p.toString();
  }
}
