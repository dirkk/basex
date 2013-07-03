package org.basex.server.replication;

import org.basex.server.*;

import akka.actor.*;
import akka.contrib.pattern.*;
import akka.event.*;

public class SubscriberActor extends UntypedActor {
  /** DistributedPubSub contrib extension to Akka. */
  private final ActorRef mediator;
  /** Publish to this id. */
  private final String topic;
  /** Logging adapter. */
  private final LoggingAdapter log;
 
  /**
   * Create Props for the subscribing actor.
   * @param t topic to subscribe to.
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final String t) {
    return Props.create(ClientHandler.class, t);
  }
  
  /**
   * Constructor.
   * @param t id to subscribe to.
   */
  public SubscriberActor(final String t) {
    topic = t;
    log = Logging.getLogger(getContext().system(), this);

    mediator = DistributedPubSubExtension.get(getContext().system()).mediator();
    mediator.tell(new DistributedPubSubMediator.Subscribe(topic, getSelf()),
        getSelf());
  }
  
  public void onReceive(Object msg) {
    // do something with the data
    log.info(msg.toString());
  }
}
