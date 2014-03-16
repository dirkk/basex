package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import akka.routing.FromConfig;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.server.election.ElectionActor;
import org.basex.server.election.ProcessNumber;
import org.basex.server.replication.InternalMessages.RequestStatus;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.basex.server.replication.ConnectionMessages.ConnectionStart;
import static org.basex.server.replication.ConnectionMessages.SyncFinished;
import static org.basex.server.replication.DataMessages.DataMessage;
import static org.basex.server.replication.DataMessages.DatabaseMessage;
import static org.basex.server.replication.InternalMessages.Connect;
import static org.basex.server.replication.InternalMessages.Start;
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
  /** Notify this actor reference when the connection startup is finished. */
  private ActorRef notifyStartupComplete = null;
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

    getContext().actorOf(ElectionActor.mkProps(new ProcessNumber(2, id), getSelf(), settings.TIMEOUT), "election");
    dbRouter = getContext().actorOf(DataExecutionActor.mkProps(dbCtx).withRouter(
      new FromConfig()), "dbrouter");
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
        // register yourself as member of this cluster
        cluster.registerOnMemberUp(new Runnable() {
          @Override
          public void run() {
            if (notifyStartupComplete != null) notifyStartupComplete.tell(true, getSelf());
          }
        });

        cluster.join(cluster.selfAddress());
      } else if (msg instanceof ConnectionStart) {
        log.info("Incoming connection request");
        ActorRef connHandler = getContext().actorOf(ConnectionHandlingActor.mkProps(set, dbCtx, settings.TIMEOUT));
        connHandler.forward(msg, getContext());
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
  public void onReceive(final Object msg) throws Exception {
    if (msg instanceof Start) {
      log.info("Replica set startup command");
      notifyStartupComplete = getSender();

      // start server socket
      getContext().actorOf(ServerActor.mkProps(getSelf(), ((Start) msg).getTcpSocket()), "server");

      setState(State.PRIMARY);
      getSelf().forward(msg, getContext());
    } else if (msg instanceof Connect) {
      cluster.join(((Connect) msg).getAddr());
      notifyStartupComplete = getSender();

      cluster.registerOnMemberUp(new Runnable() {
        @Override
        public void run() {
          log.info("Start a connection process.");
          Map<String, Integer> dbTimestamps = new HashMap<String, Integer>();
          for (Iterator<String> it = dbCtx.databases.listDBs().iterator(); it.hasNext(); ) {
            String db = it.next();
            dbTimestamps.put(db, 0);
          }

          getContext().actorSelection(((Connect) msg).getPath().toString())
            .tell(new ConnectionStart(id, settings.VOTING, settings.WEIGHT, dbTimestamps), getSelf());
        }
      });
    } else if (msg instanceof DatabaseMessage) {
      dbRouter.forward(msg, getContext());
    } else if (msg instanceof SyncFinished) {
      SyncFinished fin = (SyncFinished) msg;

      set.setState(fin.getState());
      set.setPrimary(fin.getPrimary());
      for (Member m : fin.getSecondaries()) {
        set.addMember(m);
      }

      setState(State.SECONDARY);
      if (notifyStartupComplete != null) notifyStartupComplete.tell(true, getSelf());
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
          Member m = new Member(getSelf(), id, settings.VOTING);
          m.setPrimary(true);
          set.addMember(m);
          try {
            dbCtx.triggers.register(new ReplicationTrigger(dbCtx.replication));
          } catch (BaseXException e) {
            e.printStackTrace();
          }
          break;
        case SECONDARY:
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
