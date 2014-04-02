package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.japi.Procedure;
import akka.routing.FromConfig;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.server.election.ElectionActor;
import org.basex.server.election.ProcessNumber;
import org.basex.server.replication.InternalMessages.RequestStatus;
import org.basex.server.replication.tcp.ServerActor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.basex.server.replication.ConnectionMessages.ConnectionStart;
import static org.basex.server.replication.ConnectionMessages.SyncFinished;
import static org.basex.server.replication.DataMessages.DataMessage;
import static org.basex.server.replication.DataMessages.DatabaseMessage;
import static org.basex.server.replication.InternalMessages.Start;
import static org.basex.server.replication.Settings.SettingsProvider;

/**
 * Replication actor. Root actor for BaseX replication system.
 *
 */
public class ReplicationActor extends UntypedActor {
  /** Replication states. */
  public enum State { UNCONNECTED, PRIMARY, SECONDARY }
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
  /** Handles the notification for startup of the replication system. */
  private final Notifier notifier;
  /** Settings from the application.conf. */
  public final SettingsImpl settings = SettingsProvider.get(getContext().system());
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
    notifier = new Notifier();
  }

  @Override
  public void preStart() {
    log.debug("Replication actor at path {} started.", cluster.selfAddress());
    id = cluster.selfAddress().toString();

    getContext().actorOf(ElectionActor.mkProps(new ProcessNumber(2, id), getSelf(), settings.TIMEOUT), "election");
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
            try {
              notifier.setSync(true);
            } catch (BaseXException e) {
              log.error("Could not notify the process of successful connection attempt.");
            }
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
  private Procedure<Object> unconnectedProcedure = new Procedure<Object>() {
    @Override
    public void apply(final Object msg) throws Exception {
    if (msg instanceof Start) {
      notifier.setActor(getSender());

      // start server socket
      getContext().actorOf(ServerActor.mkProps(dbCtx, getSelf(), ((Start) msg).getTcpAddr()), "server");
      dbRouter = getContext().actorOf(DataExecutionActor.mkProps(dbCtx).withRouter(
        new FromConfig()), "dbrouter");


      if (((Start) msg).getRemoteAddr() == null) {
        setState(State.PRIMARY);
        getSelf().forward(msg, getContext());
      } else {
        log.info("Join the replica set at {}", ((Start) msg).getRemoteAddr());
        cluster.join(((Start) msg).getRemoteAddr());

        cluster.registerOnMemberUp(new Runnable() {
          @Override
          public void run() {
            log.info("Start a connection process.");
            Map<String, Integer> dbTimestamps = new HashMap<String, Integer>();
            for (Iterator<String> it = dbCtx.databases.listDBs().iterator(); it.hasNext(); ) {
              String db = it.next();
              dbTimestamps.put(db, 0);
            }

            getContext().actorSelection(((Start) msg).getRemotePath().toString())
              .tell(new ConnectionStart(id, settings.VOTING, settings.WEIGHT, dbTimestamps), getSelf());
          }
        });
      }
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
      notifier.setSync(true);
    } else {
      handleAll(msg);
    }
    }
  };


  @Override
  public void onReceive(final Object msg) throws Exception {
    getContext().become(unconnectedProcedure);
    getSelf().forward(msg, getContext());
  }

  /**
   * Incoming message operations which are equal for all states of the replication actor.
   * @param msg incoming message
   */
  private void handleAll(Object msg) {
    if (msg instanceof RequestStatus) {
      System.out.println(set);
      getSender().tell(new InternalMessages.StatusMessage(set.getState(), set.getPrimary(), set.getSecondaries()), getSelf());
    } else if (msg instanceof Tcp.Bound) {
      try {
        notifier.setServer(true);
      } catch (BaseXException e) {
        log.error("Could not notify the process of successful connection attempt.");
      }
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
          log.warning("Should not change to UNCONNECTED");
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

  private class Notifier {
    /** Notify this actor reference when the connection startup is finished. */
    private ActorRef actor;
    /** TCP server bound. */
    private boolean server = false;
    /** Sync finished. */
    private boolean sync = false;
    /** Already notified? */
    private boolean notified = false;

    public void setActor(ActorRef actor) throws BaseXException {
      this.actor = actor;
      tell(true);
    }

    public void setServer(boolean server) throws BaseXException {
      this.server = server;
      tell(true);
    }

    public void setSync(boolean sync) throws BaseXException {
      this.sync = sync;
      tell(true);
    }

    private void tell(final boolean result) throws BaseXException {
      if (notified) throw new BaseXException("Notification already sent.");

      if (server && sync && actor != null) {
        notified = true;
        actor.tell(result, getSelf());
      }
    }
  }
}
