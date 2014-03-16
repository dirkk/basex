package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.io.TcpMessage;

import java.net.InetSocketAddress;

import static akka.io.Tcp.*;

/**
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class ServerActor extends UntypedActor {
  /** Manager. */
  final ActorRef manager;
  /** Socket. */
  final InetSocketAddress addr;

  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final ActorRef manager, final InetSocketAddress addr) {
    return Props.create(ServerActor.class, manager, addr);
  }

  public ServerActor(final ActorRef manager, final InetSocketAddress addr) {
    this.manager = manager;
    this.addr = addr;
  }

  @Override
  public void preStart() throws Exception {
    final ActorRef tcp = get(getContext().system()).manager();
    tcp.tell(TcpMessage.bind(getSelf(), addr, 100), getSelf());
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof Bound) {
      manager.tell(msg, getSelf());
    } else if (msg instanceof CommandFailed) {
      getContext().stop(getSelf());
    } else if (msg instanceof Connected) {
      final Connected conn = (Connected) msg;
      manager.tell(conn, getSelf());
      //final ActorRef handler = getContext().actorOf(
      //  Props.create(SimplisticHandler.class));
      //getSender().tell(TcpMessage.register(handler), getSelf());
    }
  }
}


