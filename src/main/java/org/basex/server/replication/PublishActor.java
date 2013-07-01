package org.basex.server.replication;

import org.basex.server.*;
import org.basex.server.client.*;

import akka.actor.*;
import akka.contrib.pattern.*;
import akka.event.*;

/**
 * This actor is used by a server instance to publish the changed data to subscribers,
 * i.e. slaves registered at the master for data duplication and therefore fault
 * tolerance.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class PublishActor extends UntypedActor {
  /** DistributedPubSub contrib extension to Akka. */
  private final ActorRef mediator;
  /** Publish to this id. */
  private final String topic;
  /** Logging adapter. */
  private final LoggingAdapter log;
 
  /**
   * Create Props for the publishing actor.
   * @param t topic to sent to.
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final String t) {
    return Props.create(ClientHandler.class, t);
  }
  
  /**
   * Constructor.
   * @param t id to publish to.
   */
  public PublishActor(final String t) {
    topic = t;
    mediator = DistributedPubSubExtension.get(getContext().system()).mediator();
    log = Logging.getLogger(getContext().system(), this);
  }
  
  public void onReceive(Object msg) {
    mediator.tell(new DistributedPubSubMediator.Publish(topic, msg), 
      getSelf());
  }
}
