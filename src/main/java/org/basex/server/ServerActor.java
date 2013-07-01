package org.basex.server;

import static org.basex.core.Text.*;

import java.net.*;
import org.basex.core.*;
import org.basex.server.replication.*;
import org.basex.util.*;

import akka.actor.*;
import akka.event.*;
import akka.io.*;
import akka.io.Tcp.Bound;
import akka.io.Tcp.CommandFailed;
import akka.io.Tcp.Connected;

/**
 * The server actor, responsible for binding to a port at the server side and
 * listening to incoming connections.
 * 
 * Each connection (including the authentication management) will then be passed
 * over to a {@link ClientHandler} actor.
 * 
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ServerActor extends UntypedActor {
  /** Server listening address. */
  private final InetSocketAddress serverAddr;
  /** Event address. */
  private final InetSocketAddress eventAddr;
  /** Replication: Master address. */
  private final InetSocketAddress masterAddr;
  /** Logging adapter. */
  private final LoggingAdapter log;
  /** Database context. */
  private final Context dbContext;
  /** Bound listener. */
  private ActorRef boundListener;
  /** Is sucessfully bound. */
  private boolean bound = false;
  /** Publishing actor. */
  private ActorRef publisher;

  /**
   * Create Props for the server actor.
   * @param l listening address for incoming connections
   * @param e event server address
   * @param ctx database context
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final InetSocketAddress l, final InetSocketAddress e,
      final Context ctx) {
    return Props.create(ServerActor.class, l, e, ctx);
  }

  /**
   * Constructor.
   * @param s listening address
   * @param e event server address
   * @param ctx database context
   */
  public ServerActor(final InetSocketAddress s, final InetSocketAddress e, final Context ctx) {
    this(s, e, null, ctx);
  }
  
  /**
   * Constructor.
   * @param s server address
   * @param e event server address
   * @param m master address, if any
   * @param ctx database context
   */
  public ServerActor(final InetSocketAddress s, final InetSocketAddress e,
      final InetSocketAddress m, final Context ctx) {
    serverAddr = s;
    eventAddr = e;
    masterAddr = m;
    dbContext = ctx;
    log = Logging.getLogger(getContext().system(), this);
    
    if (m != null)
      initPublishing();
  }

  @Override
  public void preStart() throws Exception {
    Tcp.get(getContext().system()).manager()
      .tell(TcpMessage.bind(getSelf(), serverAddr, 100), getSelf());
    
    getContext().actorOf(EventActor.mkProps(eventAddr),
        "events"
        );
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof String && ((String) msg).equalsIgnoreCase("bound")) {
      if (bound)
        getSender().tell(true, getSelf());
      else
        boundListener = getSender();
    } else if (msg instanceof Bound) {
      Bound b = (Bound) msg;

      log.info("Server bound to {} ", b.localAddress());
      log.info(CONSOLE + Util.info(SRV_STARTED_PORT_X, b.localAddress().getPort()), SERVERMODE);
      bound = true;
      if (boundListener != null)
        boundListener.tell(true, getSelf());
    } else if (msg instanceof CommandFailed) {
      log.error("TCP command failed: {}", msg.toString());
      getContext().stop(getSelf());
    } else if (msg instanceof Connected) {
      final Connected conn = (Connected) msg;
      log.info("Connection from {}", conn.remoteAddress());
      final ActorRef handler = getContext().actorOf(ClientHandler.mkProps(dbContext));
      getSender().tell(TcpMessage.register(handler), getSelf());
      handler.tell("connect", getSender());
    } else if (msg instanceof Terminated) {
      log.error("The actor {} terminated.", ((Terminated) msg).actor().path());
    } else {
      if (publisher != null) {
        publisher.tell(msg, getSelf());
      }
    }
  }

  /**
   * Returns a unique identifier of this server instance.
   * @return unique id
   */
  private String getId() {
    return serverAddr.toString();
  }
  
  /**
   * Initialize the publishing actor.
   */
  private void initPublishing() {
    publisher = getContext().actorOf(PublishActor.mkProps(getId()), "publisher");
  }
}
