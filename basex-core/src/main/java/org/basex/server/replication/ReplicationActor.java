package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import akka.routing.FromConfig;
import akka.util.Timeout;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.server.election.ElectionActor;
import org.basex.server.election.ProcessNumber;
import org.basex.server.replication.ConnectionMessages.ConnectionResponse;
import org.basex.server.replication.ConnectionMessages.ConnectionStart;
import org.basex.server.replication.InternalMessages.RequestStatus;
import org.basex.server.replication.InternalMessages.StartSet;
import org.basex.server.replication.InternalMessages.StatusMessage;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.basex.server.replication.ConnectionMessages.SyncFinished;
import static org.basex.server.replication.DataMessages.DataMessage;
import static org.basex.server.replication.InternalMessages.Start;
import static org.basex.server.replication.ReplicaSet.ReplicaSetState.RUNNING;
import static org.basex.server.replication.Settings.SettingsProvider;

/**
 * Replication actor. Root actor for BaseX replication system.
 *
 */
public class ReplicationActor extends UntypedActor {
  /** Replication states. */
  public enum State {
    UNCONNECTED, PRIMARY, SECONDARY
  }
  /** Finite State Machine. */
  private State state = State.UNCONNECTED;
  /** Replica set. */
  private ReplicaSet set = new ReplicaSet();
  /** Database context. */
  private final Context dbCtx;
  /** ID. */
  private String id;
  /** Cluster. */
  private Cluster cluster = Cluster.get(getContext().system());
  /** Router for database operations, just relevant for a Secondary. */
  private ActorRef dbRouter;
  /** Settings from the application.conf. */
  public final SettingsImpl settings = SettingsProvider.get(getContext().system());
  /** Timeout. */
  private final Timeout timeout = new Timeout(Duration.create(5, "seconds"));
  /** Logging. */
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final Context dbContext) {
    return Props.create(ReplicationActor.class, dbContext);
  }

  /**
   * Constructor
   * @param dbContext database context
   */
  public ReplicationActor(final Context dbContext) {
    this.dbCtx = dbContext;
  }

  @Override
  public void preStart() {
    log.debug("Replication actor at path {} started.", cluster.selfAddress());
    id = cluster.selfAddress().toString();

    getContext().actorOf(ElectionActor.mkProps(new ProcessNumber(2, id), getSelf(), timeout), "election");
  }

  @Override
  public void postStop() {
  }

  /**
   * State machine for a Primary within a replica set. Only one Primary
   * is allowed within a replica set at any time. Primary is chosen by
   * election using {@link org.basex.server.election.ElectionActor}.
   *
   * The Primary is the only member within a replica set which is writable.
   */
  private Procedure<Object> primaryProcedure = new Procedure<Object>() {
    @Override
    public void apply(Object msg) throws Exception {
      if (msg instanceof Start) {
        // TODO for now we wait till the primary issues the startup, so the replication actors at the secondaries are able to start up
        set.setPrimary(new Member(getSelf(), id, settings.VOTING));
        getContext().system().scheduler().scheduleOnce(
          Duration.create(500, TimeUnit.MILLISECONDS),
          new Runnable() {
            @Override
            public void run() {
              cluster.sendCurrentClusterState(getSelf());
            }
          }, getContext().system().dispatcher());
      } else if (msg instanceof CurrentClusterState) {
        log.info("A replica set is going up. Connect all members to the replica set.");
        CurrentClusterState state = (CurrentClusterState) msg;

        for (akka.cluster.Member member : state.getMembers()) {
          if (!member.address().equals(cluster.selfAddress())) {
            ActorRef ref = getContext().actorOf(ConnectionHandlingActor.mkProps(dbCtx, timeout));
            ref.tell(member, getSelf());
          }
        }

        //electionActor.tell(new Init(electionMembers), getSelf());
      } else if (msg instanceof StartSet) {
        // the replica set is ready, go to up state
        log.info("Put replica set to RUNNING");
        set.setState(RUNNING);
        for (Member m : set.getSecondaries()) {
          log.info("Send StatusMessage to {}, path {}", m.getActor(), m.getActor().path());
          m.getActor().tell(new StatusMessage(set.getState(), set.getPrimary(), set.getSecondaries()), getSelf());
        }

        //electionActor.tell(new Init(getVotingMembers()), getSelf());
      } else if (msg instanceof Member) {
        Member newMember = (Member) msg;

        log.info("New member {} joined the replica set.", newMember.getActor().path());
        set.addMember(newMember);
      } else if (msg instanceof DataMessage) {
        log.info("Got datamessage, {} secondaries", set.getSecondaries().size());
        // publish to all secondaries
        for (Member m : set.getSecondaries()) {
          m.getActor().tell(msg, getSelf());
        }
      } else {
        handleAll(msg);
      }
    }
  };

  /**
   * State machine for a Secondary within a replica set. A secondary is read-only
   * and will replicate all write updates from the Primary.
   */
  private Procedure<Object> secondaryProcedure = new Procedure<Object>() {
    @Override
    public void apply(Object msg) throws Exception {
      if (msg instanceof DataMessage) {
        dbRouter.forward(msg, getContext());
      } else {
        handleAll(msg);
      }
    }
  };

  /**
   * State machine for an unconnected replication actor. Can either be voted to be Primary
   * or be a Secondary. Is non-operational and read-only.
   *
   * @param msg incoming message
   * @throws Exception exception
   */
  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof Start) {
      log.info("Replica set startup command");
      setState(State.PRIMARY);
      getSelf().forward(msg, getContext());
    } else if (msg instanceof ConnectionStart)  {
      log.info("Got a connection start from {}", getSender().path());
      Map<String, Integer> dbTimestamps = new HashMap<String, Integer>();
      for (Iterator<String> it = dbCtx.databases.listDBs().iterator(); it.hasNext(); ) {
        String db = it.next();
        dbTimestamps.put(db, 0);
      }

      getSender().tell(new ConnectionResponse(id, true, 1, dbTimestamps), getSelf());
    } else if (msg instanceof StatusMessage) {
      StatusMessage sm = (StatusMessage) msg;
      log.info("Status: {}", sm);

      if (state == State.PRIMARY) {
        setState(State.PRIMARY);
      } else {
        setState(State.SECONDARY);
      }

      set.setState(sm.getState());
    } else if (msg instanceof SyncFinished) {
      setState(State.SECONDARY);
    } else {
      handleAll(msg);
    }
  }

  /**
   * Incoming message operations which are equal for all states of the replication actor.
   * @param msg incoming message
   */
  private void handleAll(Object msg) {
    if (msg instanceof RequestStatus) {
      getSender().tell(toString(), getSelf());
    } else {
      unhandled(msg);
    }
  }

  /**
   * Return the state of the replication.
   * @return state
   */
  private State getState() {
    return state;
  };

  /**
   * State machine transition from one state to another. The following changes are possible:
   *
   * <ul>
   *   <li><i>UNCONNECTED -> PRIMARY</i> A new member is voted Primary</li>
   *   <li><i>UNCONNECTED -> SECONDARY</i> A new member becomes a Secondary</li>
   *   <li><i>SECONDARY -> PRIMARY</i> The old Primary failed, so this one was elected as replacement.</li>
   *   <li><i>PRIMARY -> SECONDARY</i> The current Primary voluntarily stepped down and become a Secondary.</li>
   * </ul>
   * @param s new state
   */
  protected  synchronized void setState(State s) {
    if (state != s) {
      log.info("System {} goes from state {} to {}", cluster.selfAddress(), state, s);
      state = s;

      switch (s) {
        case UNCONNECTED:
          log.error("Should not change to UNCONNECTED");
          break;
        case PRIMARY:
          getContext().become(primaryProcedure);
          try {
            dbCtx.triggers.register(new ReplicationTrigger(dbCtx.replication));
          } catch (BaseXException e) {
            e.printStackTrace();
          }
          break;
        case SECONDARY:
          dbRouter = getContext().actorOf(DataExecutionActor.mkProps(dbCtx).withRouter(
            new FromConfig()), "dbrouter");
          getContext().become(secondaryProcedure);
          break;
      }
    }
  }

  @Override
  public String toString() {
    return set.toString();
  }
}
