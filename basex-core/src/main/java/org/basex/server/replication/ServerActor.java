package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.TcpMessage;
import org.basex.core.Context;

import java.net.InetSocketAddress;

import static akka.io.Tcp.*;

/**
 * Server actor to handle client connecting to a {@link org.basex.BaseXMember}. Binds to a TCP
 * socket.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class ServerActor extends UntypedActor {
  /** TCP manager. */
  private final ActorRef tcp;
  /** Manager. */
  private final ActorRef manager;
  /** Socket. */
  private final InetSocketAddress addr;
  /** Database context. */
  private final Context dbCtx;
  /** Logging. */
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final Context c, final ActorRef m, final InetSocketAddress a) {
    return Props.create(ServerActor.class, c, m, a);
  }

  public ServerActor(final Context ctx, final ActorRef manager, final InetSocketAddress addr) {
    this.dbCtx = ctx;
    this.manager = manager;
    this.addr = addr;
    tcp = get(getContext().system()).manager();
  }

  @Override
  public void preStart() throws Exception {
    tcp.tell(TcpMessage.bind(getSelf(), addr, 0), getSelf());
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof Bound) {
      //manager.tell(msg, getSelf());
      log.info("Server bound on {}", ((Bound) msg).localAddress());
    } else if (msg instanceof CommandFailed) {
      getContext().stop(getSelf());
    } else if (msg instanceof Connected) {
      final Connected conn = (Connected) msg;
      log.info("Incoming connection from {}", conn.remoteAddress());
      //manager.tell(conn, getSelf());
      final ActorRef handler = getContext().actorOf(ClientListenerActor.mkProps(dbCtx, getSender()));
      getSender().tell(TcpMessage.register(handler), getSelf());
    }
  }
}


