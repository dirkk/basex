package org.basex.server.replication;

import org.basex.server.election.ElectionMember;
import org.basex.server.election.ProcessNumber;
import org.basex.util.Prop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Replica set, consisting of at most one Primary and a undefined number of Secondaries.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class ReplicaSet {
  /** Replica set states. */
  public enum ReplicaSetState {
    UNCONNECTED, RUNNING, READONLY
  }
  /** Current replica set state. */
  private ReplicaSetState state = ReplicaSetState.UNCONNECTED;
  /** Primary, if any. */
  private Member primary;
  /** Map of all secondaries, key is member ID. */
  private List<Member> secondaries = new ArrayList<Member>();

  /**
   * Set the primary of the replica set.
   * @param primary new primary
   */
  public synchronized void setPrimary(Member primary) {
    this.primary = primary;
  }

  /**
   * Set a new state for the replica set
   * @param state new state
   */
  public synchronized void setState(ReplicaSetState state) {
    this.state = state;
  }

  /**
   * Add a new secondary to the replica set.
   * @param newMember new secondary
   */
  private synchronized void addSecondary(final Member newMember) {
    secondaries.add(newMember);
  }

  /**
   * The replica set is currently in state {@code UNCONNECTED}. Check, if this still apllies and if not,
   * go to the {@code RUNNING} state.
   */
  private void checkState() {
    if (getPrimary() != null && getSecondaries().size() > 0) setState(ReplicaSetState.RUNNING);
  }

  /**
   * Add a new member to the replica set, either a primary or a secondary.
   * @param newMember new member
   */
  public synchronized void addMember(final Member newMember) {
    if (newMember.isPrimary()) {
      setPrimary(newMember);
    } else {
      addSecondary(newMember);
    }

    if (getState() == ReplicaSetState.UNCONNECTED) checkState();
  }

  /**
   * Remove a secondary from the replica set.
   * @param id ID of the member to remove
   * @return success
   */
  public synchronized boolean removeSecondary(final String id) {
    boolean found = false;

    for (Member m : secondaries) {
      if (m.getId().equals(id)) {
        found = true;
        secondaries.remove(m);
      }
    }

    return found;
  }

  /**
   * Get the current primary in the replica set.
   * @return primary
   */
  public Member getPrimary() {
    return primary;
  }

  /**
   * Get a list of all secondaries.
   * @return list of secondaries
   */
  public List<Member> getSecondaries() {
    return secondaries;
  }

  /**
   * Returns the current operational state of the replica set.
   * @return state
   */
  public ReplicaSetState getState() {
    return state;
  }

  /**
   * Get a list of all members which can vote in an election.
   *
   * @return list of voting members
   */
  public List<ElectionMember> getVotingMembers() {
    List<ElectionMember> votingMembers = new ArrayList<ElectionMember>();

    if (primary != null && primary.isVoting()) {
      votingMembers.add(new ElectionMember(primary.getActor(), new ProcessNumber(primary.getWeight(), primary.getId())));
    }

    for (Member m : secondaries) {
      if (m.isVoting()) {
        votingMembers.add(new ElectionMember(m.getActor(), new ProcessNumber(m.getWeight(), m.getId())));
      }
    }

    return votingMembers;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("State: " + getState() + Prop.NL);
    b.append("Primary" + Prop.NL + getPrimary() + Prop.NL);
    b.append("Number of secondaries: " + getSecondaries().size() + Prop.NL);
    for (Iterator<Member> it = getSecondaries().iterator(); it.hasNext(); ) {
      Member m = it.next();
      b.append("Secondary " + Prop.NL + m + Prop.NL);
    }

    return b.toString();
  }

}
