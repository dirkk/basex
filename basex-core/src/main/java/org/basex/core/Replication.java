package org.basex.core;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.basex.server.replication.DataMessages;
import org.basex.server.replication.ReplicationActor;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static akka.pattern.Patterns.ask;
import static org.basex.server.replication.InternalMessages.RequestStatus;
import static org.basex.server.replication.InternalMessages.Start;

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
  public static final int PROTOCOL_V1 = 0x00;
  public static final int HASH_MD5 = 0x00;
  /** BaseX replication system name. */
  public static String SYSTEM_NAME = "replBaseX";
  /** running? . */
  private boolean running;
  /** Akka system. */
  private ActorSystem system;
  /** Replication actor. */
  private ActorRef repl;
  /** Default timeout. */
  private static Timeout TIMEOUT = new Timeout(Duration.create(10, TimeUnit.SECONDS));

  /**
   * Constructor.
   */
  protected Replication() {
    running = false;
  }

  private void systemStart(final Context context, final InetSocketAddress akka) {
    if (running) return;

    running = true;
    Config hardConfig = ConfigFactory.parseString(
      "replication.akka.remote.netty.tcp.host=\"" + akka.getHostString() + "\"," +
        "replication.akka.remote.netty.tcp.port=" + akka.getPort());
    Config regularConfig = ConfigFactory.load();
    Config completeConfig = ConfigFactory.load(hardConfig.withFallback(regularConfig));
    system = ActorSystem.create(SYSTEM_NAME, completeConfig.getConfig("replication"));

    repl = system.actorOf(ReplicationActor.mkProps(context), "replication");
  }
  
  /**
   * Start the akka subsystem and a replication actor.
   *
   * @param context context
   * @param akka host and port for akka system
   * @param server host and port to bind to for client connections
   * @return success
   */
  public boolean start(final Context context, final InetSocketAddress akka, final InetSocketAddress server) {
    systemStart(context, akka);

    Future f = ask(repl,new Start(server, null), TIMEOUT);
    try {
      return (Boolean) Await.result(f, TIMEOUT.duration());
    } catch (Exception e) {
      return false;
    }
  }

  /**
   *
   */
  public boolean connect(final Context context, final InetSocketAddress akka, final InetSocketAddress server, final InetSocketAddress connect) {
    systemStart(context, akka);
    Future f = ask(repl,new Start(server, new Address("akka.tcp", SYSTEM_NAME, connect.getHostString(), connect.getPort())), TIMEOUT);
    try {
      return (Boolean) Await.result(f, TIMEOUT.duration());
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Get info about the replication state and replica set status.
   *
   * @return info
   */
  public String info() {
    if (repl == null)
      return "No information available";

    scala.concurrent.Future<Object> f = ask(repl, new RequestStatus(), TIMEOUT);
    try {
      return (String) Await.result(f, TIMEOUT.duration());
    } catch (Exception e) {
      return "No information available";
    }
  }

  /**
   * Is this node already started as replication instance? Can be done by running
   * the REPLICATION START command and can be stopped running REPLICATION STOP:
   */
  public boolean isEnabled() {
    return running;
  }

  public void stop() {
    system.shutdown();
  }

  public void publish(final DataMessages.DataMessage msg) {
    repl.tell(msg, ActorRef.noSender());
  }
}
