package org.basex.server.replication;

import java.net.*;

import org.basex.core.*;
import org.basex.server.*;
import org.basex.server.messages.*;

import akka.actor.*;
import akka.event.*;

/**
 * Subscribe to a master node. Get all published changes from a master.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class SubscriberActor extends UntypedActor {
  /** Database context. */
  private final Context dbContext;
  /** Logging adapter. */
  private final LoggingAdapter log;
 
  /**
   * Create Props for the subscribing actor.
   * @param ctx database context
   * @param master master address
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final Context ctx, final InetSocketAddress master) {
    return Props.create(SubscriberActor.class, ctx, master);
  }
  
  /**
   * Constructor.
   * @param ctx database context
   * @param masterAddr master address
   */
  public SubscriberActor(final Context ctx, final InetSocketAddress masterAddr) {
    dbContext = ctx;
    log = Logging.getLogger(getContext().system(), this);
    String masterPath = "akka.tcp://BaseXServer@" + masterAddr.getHostString() +
        ":" + masterAddr.getPort() + "/user/server/publisher";
    try {
      context().system().actorSelection(masterPath).tell(
          new Identify(null),
          getSelf());
    } catch (Exception ex) {
      log.info(ex.getMessage());
    }
  }
  
  public void onReceive(Object msg) {
    if (msg instanceof ActorIdentity) {
      // this is the master actor
      ActorIdentity ai = (ActorIdentity) msg;
      ActorRef master = ai.getRef();
      if (master != null) {
        master.tell(new ServerCommandMessage(InternalServerCmd.SLAVE), getSelf());
        dbContext.repl.setPublisher(master);
      }
    }
    // do something with the data
    log.info("Subscriber message: {}", msg.toString());
  }
}
