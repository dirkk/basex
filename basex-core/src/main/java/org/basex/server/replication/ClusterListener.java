package org.basex.server.replication;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.basex.core.Context;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ClusterListener extends UntypedActor {
  /** Logging. */
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  /** Cluster. */
  Cluster cluster = Cluster.get(getContext().system());

  /**
   * Create props for an actor of this type.
   *
   */
  public static Props mkProps() {
    return Props.create(ClusterListener.class);
  }

  //subscribe to cluster changes
  @Override
  public void preStart() {
    cluster.subscribe(getSelf(), MemberEvent.class);
    cluster.subscribe(getSelf(), UnreachableMember.class);
  }

  //re-subscribe when restart
  @Override
  public void postStop() {
    cluster.unsubscribe(getSelf());
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof MemberUp) {
      MemberUp mUp = (MemberUp) msg;
      log.info("Member is Up: {}", mUp.member());
      getContext().parent().tell(msg, getSelf());
    } else if (msg instanceof UnreachableMember) {
      UnreachableMember mUnreachable = (UnreachableMember) msg;
      log.info("Member detected as unreachable: {}", mUnreachable.member());

    } else if (msg instanceof MemberRemoved) {
      MemberRemoved mRemoved = (MemberRemoved) msg;
      log.info("Member is Removed: {}", mRemoved.member());

    } else if (msg instanceof MemberEvent) {
      // ignore

    } else if (msg instanceof CurrentClusterState) {
      CurrentClusterState state = (CurrentClusterState) msg;
      log.info("Cluster state: Leader: {}", state.getLeader());
      for (akka.cluster.Member m :state.getMembers()) {
        log.info("Cluster state: Member: {}", m.address());
      }
    } else {
      log.info("Member Cluster listener received following msg: {}", msg);
    }
  }
}
