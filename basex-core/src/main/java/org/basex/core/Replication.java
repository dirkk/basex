package org.basex.core;

import org.basex.server.replication.*;

/**
 * Replication infrastructure for master/slave of a replica set. A replica
 * set consists of one master, who is responsible for all updating operations,
 * and publishing the changes to the connected slaves.
 * A replica set can have a theoretically indefinite number of connected slaves.
 * To increase the throughout of the systems, the slaves can be used as
 * instances for read-only queries.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class Replication {
  /** running? . */
  private boolean running;
  /** Database context. */
  private final Context context;
  /** Master. */
  private Publisher master;
  /** Slave. */
  private Subscriber slave;
  
  /**
   * Constructor.
   * @param c database context
   */
  public Replication(final Context c) {
    running = false;
    context = c;
  }
  
  /**
   * Start this instance as a master, publishing to a replica set and the
   * connected slaves.
   * 
   * @param addr address of the message broker
   * @param name replica set name
   * @return success
   */
  public boolean startMaster(final String addr, final String name) {
    running = true;
    try {
      master = new Publisher(context, addr, name);
    } catch(BaseXException e) {
      master = null;
      running = false;
      return false;
    }

    return true;
  }
  
  /**
   * Starts this instance as a slave, which is listening on changes published
   * by a master in the replica set. The slave is running in a separate thread.
   * 
   * @param addr address of the message broker
   * @param name replica set name
   * @return success
   */
  public boolean startSlave(final String addr, final String name) {
    running = true;
    try {
      slave = new Subscriber(context, addr, name);
      new Thread(slave).start();
    } catch(BaseXException e) {
      slave = null;
      running = false;
      return false;
    }
    
    return true;
  }
  
  /**
   * Is there a master or slave instance currently running?
   * @return running
   */
  public boolean isRunning() {
    return running;
  }
  
  /**
   * Is this node currently a master instance within a replica set?
   * @return is master
   */
  public boolean isMaster() {
    if (master != null)
      return true;
    
    return false;
  }

  /**
   * Is this node currently a slave instance within a replica set?
   * @return is slave
   */
  public boolean isSlave() {
    if (slave != null)
      return true;
    
    return false;
  }
  
  /**
   * Return the address of the message broker server (should be rabbitMQ)
   * 
   * @return URI of the message broker
   */
  public String getBrokerAddress() {
    if (isMaster())
      return master.getAddress();
    
    if (isSlave())
      return slave.getAddress();
    
    return "";
  }

  /**
   * Stops the replication instance, either master or slave.
   */
  public void stop() {
    if (isRunning()) {
      if (master != null) {
        master.close();
        master = null;
        running = false;
      } else if (slave != null) {
        slave.close();
        slave = null;
        running = false;
      }
    }
  }

  /**
   * Send the given document to the message broker, if this instance is a
   * master.
   * @param o document to send
   */
  public void replicate(DocumentMessage o) {
    if (master != null) {
      master.publish(o);
    }
  }
}
