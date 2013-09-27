package org.basex.server.election;

import java.io.Serializable;
import java.util.Set;

/**
 * All messages for the election of a primary in the replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface ElectionMessages {
  /**
   * Send to an election actor to trigger an
   * election. The given members participate in the election.
   */
  public class Init implements Serializable {
    private final Set<ElectionMember> participants;

    public Init(Set<ElectionMember> participants) {
      this.participants = participants;
    }

    public Set<ElectionMember> getParticipants() {
      return participants;
    }
  }

  /**
   * @author BaseX Team 2005-12, BSD License
   * @author Dirk Kirsten
   */
  public class Announce implements Serializable {
    /** Member. */
    private final ElectionMember member;

    public Announce(final ElectionMember member) {
      this.member = member;
    }

    public ElectionMember getMember() {
      return member;
    }
  }


  /**
   * Answer an {@code Announce} message.
   *
   * @author BaseX Team 2005-12, BSD License
   * @author Dirk Kirsten
   */
  public class Ok implements Serializable {
    /** Member. */
    private final ElectionMember member;

    /**
     * Constructor.
     * @param member this member
     */
    public Ok(final ElectionMember member) {
      this.member = member;
    }

    public ElectionMember getMember() {
      return member;
    }
  }


  /**
   * Announce yourself as the new coordinator.
   *
   * @author BaseX Team 2005-12, BSD License
   * @author Dirk Kirsten
   */
  public class Coordinator implements Serializable {
    /** The new primary. */
    private final ElectionMember coordinator;

    public Coordinator(ElectionMember coordinator) {
      this.coordinator = coordinator;
    }

    public ElectionMember getCoordinator() {
      return coordinator;
    }
  }

  public class Check implements Serializable {}
}
