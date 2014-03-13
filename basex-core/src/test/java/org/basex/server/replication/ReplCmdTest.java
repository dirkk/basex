package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.RoundRobinRouter;
import akka.testkit.TestActorRef;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.basex.core.BaseXException;
import org.basex.core.Command;
import org.basex.core.Context;
import org.basex.core.cmd.*;
import org.basex.util.Performance;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Concurrency test for the ReplCmd, which executes an arbitrary number
 * of commands and should be thread-safe.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class ReplCmdTest extends SimpleSandboxTest {
  /** Number of executions. */
  private static int TRIES = 100;

  @Test
  public void runQueries() throws Exception {
    final Context context = createSandbox();
    final ArrayList<Command> cmds = new ArrayList<Command>();
    cmds.add(new CreateDB("test"));
    cmds.add(new Open("test"));

    final Client1[] cl = new Client1[10];
    for(int i = 0; i < cl.length; ++i) cl[i] = new Client1(cmds, context);

    for(final Client1 c : cl) c.start();
    for(final Client1 c : cl) c.join();
  }

  @Test
  public void runQueriesReplicated() throws BaseXException, ReplicationExceptions.ReplicationAlreadyRunningException {
    final Context ctx1 = createSandbox();
    final Context ctx2 = createSandbox();
    new ReplicationStart("127.0.0.1", 8765).execute(ctx1);
    Performance.sleep(2000);

    new ReplicationConnect("127.0.0.1", 8800, "127.0.0.1", 8765).execute(ctx2);
    Performance.sleep(2000);

    new CreateDB("test").execute(ctx1);
    for (int i = 0; i < TRIES; ++i) {
      new Add("test.xml", "<A />").execute(ctx1);
    }

    Performance.sleep(500);
  }

  @Test
  public void runQueriesMultipleDataExecutionActors() throws InterruptedException {
    final Context context = createSandbox();

    Config cfg = ConfigFactory.parseString(
      "akka.loglevel=OFF," +
        "akka.loggers = [\"akka.testkit.TestEventListener\"]," +
        "akka.stdout-loglevel=OFF");
    ActorSystem system = ActorSystem.create("testRepl", cfg);

    final Client2[] cl = new Client2[10];
    for(int i = 0; i < cl.length; ++i) cl[i] = new Client2(context, system);

    for(final Client2 c : cl) c.start();
    for(final Client2 c : cl) c.join();
  }

  @Test
  public void runQueriesWithRouter() throws InterruptedException {
    final Context context = createSandbox();

    Config cfg = ConfigFactory.parseString(
      "akka.loglevel=OFF," +
        "akka.loggers = [\"akka.testkit.TestEventListener\"]," +
        "akka.stdout-loglevel=OFF");
    ActorSystem system = ActorSystem.create("testRepl", cfg);

    ActorRef router = system.actorOf(DataExecutionActor.mkProps(context).withRouter(new RoundRobinRouter(5)));
    for (int i = 0; i < TRIES; i++) {
      router.tell(new DataMessages.AddMessage("test/test.xml", new byte[]{60, 65, 47, 62}), ActorRef.noSender());
    }

    Performance.sleep(5000);
  }



  /** Single client. */
  class Client1 extends Thread {
    /** Commands to execute. */
    private final ArrayList<Command> cmds;
    /** Database context. */
    private final Context context;

    public Client1(ArrayList<Command> cmds, Context context) {
      this.cmds = cmds;
      this.context = context;
    }

    @Override
    public void run() {
      for (int i = 0; i < TRIES; i++) {
        new ReplCmd(cmds, context).run();
      }
    }
  }

  /** Single client. */
  class Client2 extends Thread {
    /** Commands to execute. */
    private final TestActorRef<DataExecutionActor> ref;

    public Client2(final Context context, final ActorSystem system) {
      this.ref = TestActorRef.create(system, DataExecutionActor.mkProps(context));
    }

    @Override
    public void run() {
      for (int i = 0; i < (TRIES / 10); i++) {
          ref.tell(new DataMessages.AddMessage("test/test.xml", new byte[]{60, 65, 47, 62}), ActorRef.noSender());
      }
    }
  }
}
