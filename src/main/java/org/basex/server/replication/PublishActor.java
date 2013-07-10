package org.basex.server.replication;

import java.util.*;

import org.basex.core.*;
import org.basex.server.messages.*;

import akka.actor.*;
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
  /** List of connected slaves */
  private final List<ActorRef> slaves;
  /** Logging adapter. */
  @SuppressWarnings("unused")
  private final LoggingAdapter log;
 
  /**
   * Create Props for the publishing actor.
   * @param ctx database context
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final Context ctx) {
    return Props.create(PublishActor.class, ctx);
  }
  
  /**
   * Constructor.
   * @param ctx database context
   */
  public PublishActor(final Context ctx) {
    dbContext = ctx;
    slaves = new LinkedList<ActorRef>();
    log = Logging.getLogger(getContext().system(), this);
  }
  
  @Override
  public void preStart() {
    dbContext.repl.setPublisher(getSelf());
  }
  
  @Override
  public void onReceive(Object msg) {
    log.info("Publishing message: {}", msg.toString());
    if (msg instanceof ServerCommandMessage) {
      addSlave(getSender());
    } else {
      publish(msg);
    }
  }
  
  private void addSlave(final ActorRef slave) {
    slaves.add(slave);
  }
  
  private void publish(Object msg) {
    for (ActorRef act : slaves)
      act.tell(msg, getSelf());
  }
}
