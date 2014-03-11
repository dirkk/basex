package org.basex.server.replication;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Procedure;
import akka.util.Timeout;
import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.Replication;
import org.basex.core.cmd.*;
import org.basex.server.election.ElectionActor;
import org.basex.server.election.ElectionMember;
import org.basex.server.election.ProcessNumber;
import org.basex.server.replication.ConnectionMessages.ConnectionResponse;
import org.basex.server.replication.ConnectionMessages.ConnectionStart;
import org.basex.server.replication.InternalMessages.RequestStatus;
import org.basex.server.replication.InternalMessages.StartSet;
import org.basex.server.replication.InternalMessages.StatusMessage;
import org.basex.util.Prop;
import org.basex.util.Token;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.basex.server.replication.ConnectionMessages.SyncFinished;
import static org.basex.server.replication.DataMessages.DataMessage;
import static org.basex.server.replication.InternalMessages.Start;

/**
 * Replication actor. Root actor for BaseX replication system.
 *
 */
public class ReplicationActor extends UntypedActor {
  /** Replication states. */
  public enum State {
    UNCONNECTED, PRIMARY, SECONDARY
  }
  /** Cluster states. */
  public enum ClusterState {
    INACTIVE, RUNNING, READONLY
  }
  /** Current cluster state. */
  private ClusterState clusterState = ClusterState.INACTIVE;
  /** Finite State Machine. */
  private State state = State.UNCONNECTED;
  /** Database context. */
  private final Context dbCtx;
  /** This member. */
  private Member self;
  /** Primary. */
  private Member primary;
  /** Map of all secondaries, key is member ID. */
  private Map<String, Member> secondaries = new HashMap<String, Member>();
  /** Announce actor. */
  private ActorRef electionActor;
  /** ID. */
  private String id;
  /** Cluster listener. */
  private ActorRef clusterListener;
  /** Cluster. */
  private Cluster cluster = Cluster.get(getContext().system());
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

  public ReplicationActor(final Context dbContext) {
    this.dbCtx = dbContext;
  }

  @Override
  public void preStart() {
    log.info("Replication actor at path {} started.", cluster.selfAddress());
    id = cluster.selfAddress().toString();

    electionActor = getContext().actorOf(ElectionActor.mkProps(new ProcessNumber(2, id), getSelf(), timeout), "election");

    // TODO get voting value
    self = new Member(getSelf(), id, true);
  }

  @Override
  public void postStop() {
  }

  /**
   * Get the current primary in the replica set.
   * @return primary
   */
  private Member getPrimary() {
    return primary;
  }

  /**
   * Get a list of all secondaries.
   * @return list of secondaries
   */
  private List<Member> getSecondaries() {
    return new LinkedList<Member>(secondaries.values());
  }

  /**
   * The actor got a request message to send some information parameters.
   * @param msg
   */
  private void handleStatusRequest(final RequestStatus msg) {
    StringBuilder b = new StringBuilder();
    b.append("State: " + getState() + Prop.NL);
    b.append("Primary" + Prop.NL + getPrimary() + Prop.NL);
    b.append("Number of secondaries: " + getSecondaries().size() + Prop.NL);
    for (Iterator<Member> it = getSecondaries().iterator(); it.hasNext(); ) {
      Member m = it.next();
      b.append("Secondary " + Prop.NL + m + Prop.NL);
    }

    getSender().tell(b.toString(), getSelf());
  }

  private Set<ElectionMember> getVotingMembers() {
    Set<ElectionMember> set = new HashSet<ElectionMember>();

    if (primary != null && primary.isVoting()) {
      set.add(new ElectionMember(primary.getActor(), new ProcessNumber(primary.getWeight(), primary.getId())));
    }

    for (Member m : secondaries.values()) {
      if (m != null && m.isVoting()) {
        set.add(new ElectionMember(m.getActor(), new ProcessNumber(m.getWeight(), m.getId())));
      }
    }

    return set;
  }

  private Procedure<Object> primaryProcedure = new Procedure<Object>() {
    @Override
    public void apply(Object msg) throws Exception {
      if (msg instanceof Start) {
        // TODO for now we wait till the primary issues the startup, so the replication actors at the secondaries are able to start up
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
        Set<ElectionMember> electionMembers = new HashSet<ElectionMember>();

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
        clusterState = ClusterState.RUNNING;
        for (Member m : secondaries.values()) {
          log.info("Send StatusMessage to {}, path {}", m.getActor(), m.getActor().path());
          m.getActor().tell(new StatusMessage(clusterState, primary, new LinkedList<Member>(secondaries.values())), getSelf());
        }

        //electionActor.tell(new Init(getVotingMembers()), getSelf());
      } else if (msg instanceof Member) {
        Member newMember = (Member) msg;

        log.info("New member {} joined the replica set.", newMember.getActor().path());
        if (newMember.isPrimary()) {
          primary = newMember;
        } else {
          secondaries.put(newMember.getId(), newMember);
        }
      } else if (msg instanceof RequestStatus) {
        handleStatusRequest((RequestStatus) msg);
      } else if (msg instanceof DataMessage) {
        log.info("Got datamessage, {} secondaries", secondaries.values().size());
        // publish to all secondaries
        for (Member m : secondaries.values()) {
          log.info("SEC Send to {}", m.getActor());
          m.getActor().tell(msg, getSelf());
        }
      } else {
        unhandled(msg);
      }
    }
  };

  private Procedure<Object> secondaryProcedure = new Procedure<Object>() {
    @Override
    public void apply(Object msg) throws Exception {
      if (msg instanceof RequestStatus) {
        handleStatusRequest((RequestStatus) msg);
      } else if (msg instanceof DataMessage) {
        // got an update from the primary
        DataMessage dm = (DataMessage) msg;

        if (dm instanceof DataMessages.AddMessage) {
          log.info("AddMessage received");
          DataMessages.AddMessage am = (DataMessages.AddMessage) dm;

          String content = Token.string(am.getContent());
          log.info("Database: {}, Path: {}, Content: {}", am.getDatabaseName(), am.getPath(), content);

          new Open(am.getDatabaseName()).execute(dbCtx);
          new Add(am.getPath(), content).execute(dbCtx);
        } else if (dm instanceof DataMessages.UpdateMessage) {
          log.info("UpdateMessage received");
          DataMessages.UpdateMessage um = (DataMessages.UpdateMessage) dm;

          String content = Token.string(um.getContent());
          log.info("Database: {}, Path: {}, Content: {}", um.getDatabaseName(), um.getPath(), content);

          new Open(um.getDatabaseName()).execute(dbCtx);
          new Replace(um.getPath(), content).execute(dbCtx);
        } else if (dm instanceof DataMessages.RenameMessage) {
          log.info("RenameMessage received");
          DataMessages.RenameMessage rm = (DataMessages.RenameMessage) dm;
          new Rename(rm.getSource(), rm.getTarget()).execute(dbCtx);
        } else if (dm instanceof DataMessages.DeleteMessage) {
          log.info("DeleteMessage received");
          new Delete(((DataMessages.DeleteMessage) dm).getTarget()).execute(dbCtx);
        } else if (dm instanceof DataMessages.CreateDbMessage) {
          log.info("CreateDbMessage received");
          new CreateDB(((DataMessages.CreateDbMessage) dm).getName()).execute(dbCtx);
        } else if (dm instanceof DataMessages.AlterMessage) {
          log.info("AlterMessage received");
          DataMessages.AlterMessage am = (DataMessages.AlterMessage) dm;
          new AlterDB(am.getSource(), am.getTarget()).execute(dbCtx);
        } else if (dm instanceof DataMessages.DropMessage) {
          log.info("DropMessage received");
          new DropDB(((DataMessages.DropMessage) dm).getName()).execute(dbCtx);
        } else if (dm instanceof DataMessages.CopyMessage) {
          log.info("CopyMessage received");
          DataMessages.CopyMessage cm = (DataMessages.CopyMessage) dm;
          new Copy(cm.getSource(), cm.getTarget()).execute(dbCtx);
        }
      } else {
        unhandled(msg);
      }
    }
  };

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

      if (sm.getPrimary() != null && sm.getPrimary().getId().equals(id)) {
        self = sm.getPrimary();
      } else {
        for (Iterator<Member> it = sm.getSecondaries().iterator(); it.hasNext(); ) {
          Member m = it.next();
          if (m.getId().equals(id)) {
            self = m;
          }
        }
      }

      if (self.isPrimary()) {
        setState(State.PRIMARY);
      } else {
        setState(State.SECONDARY);
      }

      clusterState = sm.getState();
    } else if (msg instanceof SyncFinished) {
      setState(State.SECONDARY);
    } else if (msg instanceof RequestStatus) {
      handleStatusRequest((RequestStatus) msg);
    } else {
      unhandled(msg);
    }
  }

  public State getState() { return state; };

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
            dbCtx.triggers.register(new ReplicationTrigger(Replication.getInstance(dbCtx)));
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
}
