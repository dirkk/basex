package org.basex.server.replication;

import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.util.Timeout;
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
      "replication.BaseX.weight = 9," +
      "replication.BaseX.voting = off," +
      "replication.BaseX.timeout = 2 seconds"
    );
    Config std = ConfigFactory.load();
    Config completeConfig = ConfigFactory.load(cfg.withFallback(std)).getConfig("replication");
    ActorSystem system = ActorSystem.create("testRepl", completeConfig);

    TestActorRef<ReplicationActor> ref = TestActorRef.create(system, ReplicationActor.mkProps(createSandbox()));
    assertEquals(false, ref.underlyingActor().settings.VOTING);
    assertEquals(9, ref.underlyingActor().settings.WEIGHT);
    assertEquals(new Timeout(Duration.create(2000, TimeUnit.MILLISECONDS)), ref.underlyingActor().settings.TIMEOUT);

    system.shutdown();
  }

  @Test
  public void testDefaultSettings() throws Exception {
    Config completeConfig = ConfigFactory.load().getConfig("replication");
    ActorSystem system = ActorSystem.create("testRepl", completeConfig);

    TestActorRef<ReplicationActor> ref = TestActorRef.create(system, ReplicationActor.mkProps(createSandbox()));
    assertEquals(true, ref.underlyingActor().settings.VOTING);
    assertEquals(1, ref.underlyingActor().settings.WEIGHT);
    assertEquals(new Timeout(Duration.create(5000, TimeUnit.MILLISECONDS)), ref.underlyingActor().settings.TIMEOUT);

    system.shutdown();
  }

}
