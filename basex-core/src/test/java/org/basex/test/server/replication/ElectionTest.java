package org.basex.test.server.replication;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import akka.util.Timeout;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.basex.server.election.ElectionActor;
import org.basex.server.election.ElectionMember;
import org.basex.server.election.ElectionMessages.Coordinator;
import org.basex.server.election.ElectionMessages.Init;
import org.basex.server.election.ProcessNumber;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertEquals;


/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ElectionTest {
  /**
   * Actor system.
   */
  private static ActorSystem system;
  /** Election timeout. */
  private static Timeout timeout = new Timeout(Duration.create(100, "milliseconds"));

  @BeforeClass
  public static void setup() {
    Config cfg = ConfigFactory.parseString(
      "akka.loglevel=DEBUG," +
        "akka.loggers = [\"akka.testkit.TestEventListener\"]," +
        "akka.stdout-loglevel=DEBUG");
    system = ActorSystem.create("testRepl", cfg);
  }

  @AfterClass
  public static void teardown() {
  }

  private ElectionMember createMember(final int pNumber) {
    return createMember(pNumber, null);
  }

  private ElectionMember createMember(final int pNumber, final String name) {
    JavaTestKit probe = new JavaTestKit(system);
    final ProcessNumber pn = new ProcessNumber(pNumber, probe.getRef().path().toString());
    TestActorRef<ElectionActor> ref;
    if (name == null) {
      ref = TestActorRef.create(system, (ElectionActor.mkProps(pn, null, timeout)));
    } else {
      ref = TestActorRef.create(system, (ElectionActor.mkProps(pn, null, timeout)), name);
    }
    return ref.underlyingActor().self;
  }

  @Test
  public void electionTwoMembers() throws Exception {
    new JavaTestKit(system) {{
      // create first member and a reference to this actor to communicate with it directly
      final ProcessNumber pn =  new ProcessNumber(1, getRef().path().toString());
      final TestActorRef<ElectionActor> ref1 = TestActorRef.create(system, ElectionActor.mkProps(pn, getRef(), timeout));

      // let one member join the election set
      Set<ElectionMember> set = new HashSet<ElectionMember>();
      set.add(createMember(1));

      // trigger an election
      ref1.tell(new Init(set), ActorRef.noSender());

      expectMsgClass(duration("200 milliseconds"), Coordinator.class);
    }};
  }

  @Test
  public void electionThreeMembers() throws Exception {
    new JavaTestKit(system) {{
      // create first member and a reference to this actor to communicate with it directly
      final ProcessNumber pn =  new ProcessNumber(1, getRef().path().toString());
      final TestActorRef<ElectionActor> ref1 = TestActorRef.create(system, ElectionActor.mkProps(pn, getRef(), timeout));

      // let two members join the election set
      Set<ElectionMember> set = new HashSet<ElectionMember>();
      set.add(createMember(1));
      set.add(createMember(1));

      // trigger an election
      ref1.tell(new Init(set), ActorRef.noSender());

      expectMsgClass(duration("200 milliseconds"), Coordinator.class);
    }};
  }


  @Test
  public void electionHundredMembers() throws Exception {
    new JavaTestKit(system) {{
      // create first member and a reference to this actor to communicate with it directly
      final ProcessNumber pn =  new ProcessNumber(1, getRef().path().toString());
      final TestActorRef<ElectionActor> ref1 = TestActorRef.create(system, ElectionActor.mkProps(pn, getRef(), timeout));

      Set<ElectionMember> set = new HashSet<ElectionMember>();
      for (int i  = 1; i <= 100; ++i)
        set.add(createMember(i));

      // trigger an election
      ref1.tell(new Init(set), ActorRef.noSender());

      expectMsgClass(duration("200 milliseconds"), Coordinator.class);
    }};
  }

  @Test
  public void electionLetInitiatingMemberWin() throws Exception {
    new JavaTestKit(system) {{
      // create first member and a reference to this actor to communicate with it directly
      final ProcessNumber pn =  new ProcessNumber(3, getRef().path().toString());
      final TestActorRef<ElectionActor> ref1 = TestActorRef.create(system, ElectionActor.mkProps(pn, getRef(), timeout), "special");

      // let two members join the election set
      Set<ElectionMember> set = new HashSet<ElectionMember>();
      set.add(createMember(2));
      set.add(createMember(1));

      // trigger an election
      ref1.tell(new Init(set), ActorRef.noSender());

      final String out = new ExpectMsg<String>("match hint") {
        protected String match(Object in) {
          if (in instanceof Coordinator) {
            Coordinator c = (Coordinator) in;
            if (c.getCoordinator().getActor().toString().compareTo("TestActor[akka://testRepl/user/special]") == 0) {
              return "match";
            }
            throw noMatch();
          } else {
            throw noMatch();
          }
        }
      }.get();
      assertEquals("match", out);
    }};
  }

  @Test
  public void electionLetNoninitiatingMemberWin() throws Exception {
    new JavaTestKit(system) {{
      // create first member and a reference to this actor to communicate with it directly
      final ProcessNumber pn =  new ProcessNumber(1, getRef().path().toString());
      final TestActorRef<ElectionActor> ref1 = TestActorRef.create(system, ElectionActor.mkProps(pn, getRef(), timeout));

      // let two members join the election set
      Set<ElectionMember> set = new HashSet<ElectionMember>();
      set.add(createMember(2));
      set.add(createMember(3, "special2"));

      // trigger an election
      ref1.tell(new Init(set), ActorRef.noSender());

      final String out = new ExpectMsg<String>("match hint") {
        protected String match(Object in) {
          if (in instanceof Coordinator) {
            Coordinator c = (Coordinator) in;
            if (c.getCoordinator().getActor().toString().compareTo("TestActor[akka://testRepl/user/special2]") == 0) {
              return "match";
            }
            throw noMatch();
          } else {
            throw noMatch();
          }
        }
      }.get();
      assertEquals("match", out);
    }};
  }
}

