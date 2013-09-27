package org.basex.server.election;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import org.basex.server.election.ElectionMessages.*;

import java.util.*;

/**
 * Handle the election process to elect a new leader (<i>primary</i>) within a group of members. When a process
 * notices that the leader is no longer responding or if it is manually triggered from the application, this election
 * is trigger.
 *
 * To initiate an election a {@link }
 *
 * The election process works as follows:
 * <p/>
 * <ol>
 * <li> Any member can send a <i>Announce Start</i> message. This message is broadcasted to each
 * member in the election.</li>
 * <li> After receiving an election start, the member switches in the ELECTION state. Based on certain
 * criteria it votes for one member in the set. A member can also be non-voting, then it just responds
 * with a non-participating vote. The voting decision is send back to the initiator using an <i>election
 * vote</i> message.</li>
 * <li> The initiator collects all responses. When they arrived it elects the primary with the majority
 * of the votes. If several members have the same number of votes any of this members is randomly selected.</li>
 * <li> The new primary is broadcasted to each member in the replica set. The members transition their state
 * to the particular state (either PRIMARY or SECONDARY).</li>
 * </ol>
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ElectionActor extends UntypedActor {
  /** Logging. */
  private LoggingAdapter log = Logging.getLogger(getContext().system(), this);
  /** Actor this election belongs to. */
  public final ElectionMember self;
  /** Announce actor references for the currently running election. */
  private List<ElectionMember> members;
  /** Announce currently running? */
  private boolean running;
  /** In the currently running election, this is the member with the highest weight which did respond. */
  private ElectionMember candidate;
  /** Callback to notify of the election result, if any. */
  private final ActorRef callback;
  /** Timeout. */
  private static Timeout timeout;

  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final ProcessNumber p, final ActorRef callback, final Timeout timeout) {
    return Props.create(ElectionActor.class, p, callback, timeout);
  }

  public ElectionActor(final ProcessNumber p, final ActorRef callback, final Timeout timeout) {
    this.self = new ElectionMember(getSelf(), p);
    this.callback = callback;
    running = false;
    members = new LinkedList<ElectionMember>();
    this.timeout = timeout;
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof Init) {
      if (!running) {
        Init init = (Init) msg;

        running = true;
        log.info("Election called at {}", getSelf());

        // process members
        for (ElectionMember m : init.getParticipants()) {
          if (m.getActor() != self.getActor()) {
            members.add(m);
          }
        }

        // send the election start to all members with a higher process number
        ElectionMember m;
        boolean send = false;
        for (Iterator<ElectionMember> it = members.iterator(); it.hasNext() && self.getProcessNumber().compareTo((m = it.next()).getProcessNumber()) < 0; ) {
          log.info("Send Start to Member {} with ProcessNumber {}. This member has ID {} and ProcessNumber {}",
            m.getActor(), m.getProcessNumber(), self.getActor(), self.getProcessNumber());
          m.getActor().tell(new Announce(self), getSelf());
          send = true;
        }

        if (send) {
          // schedule check for responses
          getContext().system().scheduler().scheduleOnce(timeout.duration(), getSelf(),
                new Check(), getContext().system().dispatcher(), null);
        } else {
          getSelf().tell(new Check(), getSelf());
        }
      }
    } else if (msg instanceof Announce) {
      ElectionMember announcer = ((Announce) msg).getMember();
      log.debug("Got election start msg from {}.", announcer);

      if (announcer.getProcessNumber().compareTo(self.getProcessNumber()) < 0) {
        getSender().tell(new Ok(self), getSelf());
      }
    } else if (msg instanceof Ok) {
      ElectionMember responder = ((Ok) msg).getMember();
      log.debug("Got OK message from {}", getSender());

      if (candidate == null || responder.getProcessNumber().compareTo(candidate.getProcessNumber()) > 0) {
        candidate = responder;
      }
    } else if (msg instanceof Check) {
      log.debug("Check");
      // if no candidate found, this member must be the new primary. Announce yourself
      if (candidate == null) {
        candidate = self;
      }

      // send new primary to all members
      for (ElectionMember m : members) {
        m.getActor().tell(new Coordinator(candidate), getSelf());
      }
      getSelf().tell(new Coordinator(candidate), getSelf());
    } else if (msg instanceof Coordinator) {
      Coordinator ec = (Coordinator) msg;
      // election is finished
      running = false;
      members = null;

      log.info("Member {} (Weight: {}) is the new primary.", ec.getCoordinator().getActor(), ec.getCoordinator().getProcessNumber());

      if (callback != null) callback.forward(msg, getContext());
    } else {
      unhandled(msg);
    }
  }
}
