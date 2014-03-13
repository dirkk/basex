package org.basex.server.replication;

import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * BaseX specific settings from the akka configuration.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class SettingsTest extends SimpleSandboxTest {
  @Test
  public void testSettingsPresent() throws Exception {
    Config cfg = ConfigFactory.parseString(
      "akka.loglevel=ERROR," +
        "akka.loggers = [\"akka.testkit.TestEventListener\"]," +
        "akka.actor.provider = \"akka.cluster.ClusterActorRefProvider\"," +
        "akka.stdout-loglevel=ERROR," +
        "BaseX.weight = 9," +
        "BaseX.voting = off," +
        "BaseX.timeout = 2 seconds"
    );
    ActorSystem system = ActorSystem.create("testRepl", cfg);

    TestActorRef<ReplicationActor> ref = TestActorRef.create(system, ReplicationActor.mkProps(createSandbox()));
    assertEquals(ref.underlyingActor().settings.VOTING, false);
    assertEquals(ref.underlyingActor().settings.WEIGHT, 9);
    assertEquals(ref.underlyingActor().settings.TIMEOUT, Duration.create(2000, TimeUnit.MILLISECONDS));

    system.shutdown();
  }

  @Test
  public void testDefaultSettings() throws Exception {
    Config cfg = ConfigFactory.parseString(
      "akka.loglevel=ERROR," +
        "akka.loggers = [\"akka.testkit.TestEventListener\"]," +
        "akka.actor.provider = \"akka.cluster.ClusterActorRefProvider\"," +
        "akka.stdout-loglevel=ERROR"
    );
    ActorSystem system = ActorSystem.create("testRepl", cfg);

    TestActorRef<ReplicationActor> ref = TestActorRef.create(system, ReplicationActor.mkProps(createSandbox()));
    assertEquals(ref.underlyingActor().settings.VOTING, true);
    assertEquals(ref.underlyingActor().settings.WEIGHT, 1);
    assertEquals(ref.underlyingActor().settings.TIMEOUT, Duration.create(5000, TimeUnit.MILLISECONDS));

    system.shutdown();
  }

}
