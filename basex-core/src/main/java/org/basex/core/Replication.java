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
import scala.concurrent.duration.Duration;

import static akka.pattern.Patterns.ask;
import static org.basex.server.replication.InternalMessages.*;
import static org.basex.server.replication.ReplicationExceptions.ReplicationAlreadyRunningException;

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
  /** BaseX replication system name. */
  public static String SYSTEM_NAME = "replBaseX";
  /** running? . */
  private boolean running;
  /** Akka system. */
  private ActorSystem system;
  /** Replication actor. */
  private ActorRef repl;
  /** Default timeout. */
  private Timeout timeout = new Timeout(Duration.create(5, "seconds"));

  /**
   * Constructor.
   */
  protected Replication() {
    running = false;
  }
  
  /**
   * Start the replication and subsequently the akka subsystem.
   *
   * @param context context
   * @param host host name
   * @param port port
   * @return success
   */
  public boolean start(final Context context, final String host, final int port) throws ReplicationAlreadyRunningException {
    systemStart(host, port);

    repl = system.actorOf(ReplicationActor.mkProps(context), "replication");
    repl.tell(new Start(), ActorRef.noSender());

    return true;
  }

  private void systemStart(final String host, final int port) throws ReplicationAlreadyRunningException {
    if (running) {
      throw new ReplicationAlreadyRunningException();
    } else {
      running = true;
      Config hardConfig = ConfigFactory.parseString(
        "replication.akka.remote.netty.tcp.host=\"" + host + "\"," +
          "replication.akka.remote.netty.tcp.port=" + port);
      Config regularConfig = ConfigFactory.load();
      Config completeConfig = ConfigFactory.load(hardConfig.withFallback(regularConfig));
      system = ActorSystem.create(SYSTEM_NAME, completeConfig.getConfig("replication"));
    }
  }

  /**
   *
   */
  public void connect(final Context context, final String localHost, final int localPort,
                      final String remoteHost, final int remotePort) throws ReplicationAlreadyRunningException {
    systemStart(localHost, localPort);

    Address addr = new Address("akka.tcp", "replBaseX", remoteHost, remotePort);
    repl = system.actorOf(ReplicationActor.mkProps(context), "replication");
    repl.tell(new Connect(addr), ActorRef.noSender());
  }

  /**
   * Get info about the replication state and replica set status.
   *
   * @return info
   */
  public String info() {
    if (repl == null)
      return "No information available";

    scala.concurrent.Future<Object> f = ask(repl, new RequestStatus(), timeout);
    try {
      return (String) Await.result(f, timeout.duration());
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
