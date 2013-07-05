package org.basex.server.replication;

import org.basex.core.*;

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
  /** Database context. */
  private final Context dbContext;
  /** DistributedPubSub contrib extension to Akka. */
  private final ActorRef mediator;
  /** Publish to this id. */
  private final String topic;
  /** Logging adapter. */
  private final LoggingAdapter log;
 
  /**
   * Create Props for the publishing actor.
   * @param ctx database context
   * @param t topic to sent to.
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final Context ctx, final String t) {
    return Props.create(PublishActor.class, ctx, t);
  }
  
  /**
   * Constructor.
   * @param ctx database context
   * @param t id to publish to.
   */
  public PublishActor(final Context ctx, final String t) {
    dbContext = ctx;
    topic = t;
    mediator = DistributedPubSubExtension.get(getContext().system()).mediator();
    log = Logging.getLogger(getContext().system(), this);
  }
  
  @Override
  public void onReceive(Object msg) {
    mediator.tell(new DistributedPubSubMediator.Publish(topic, msg), 
      getSelf());
  }
}
