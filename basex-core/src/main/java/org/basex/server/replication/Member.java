package org.basex.server.replication;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import org.basex.core.Prop;

import java.io.Serializable;

/**
 * A member within a replica set. It can either be a primary or a
 * secondary.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class Member implements Serializable {
  /** Reference to the remote replication actor. */
  private final ActorRef actor;
  /** ID. Derived from akka cluster address. */
  private final String id;
  /** Votes in a primary election. */
  private final boolean isVoting;
  /** Is primary. */
  private boolean isPrimary;
  /** Weight. The Member in the set with the highest weight will be elected as primary. */
  private int weight;

  /**
   * Constructor.
   * @param actor remote replication actor reference
   * @param id member ID
   * @param voting election voting
   */
  public Member(final ActorRef actor, final String id, final boolean voting) {
    this.actor = actor;
    this.id = id;
    this.isVoting = voting;

    this.weight = 1;
  }

  /**
   * Return the member ID.
   *
   * @return id
   */
  public String getId() { return id; }

  /**
   * Return the Actor reference.
   * @return replication actor reference
   */
  public ActorRef getActor() {
    return actor;
  }

  /**
   * Get the weight.
   * @return weight
   */
  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  public boolean isVoting() {
    return isVoting;
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public void setPrimary(boolean isPrimary) {
    this.isPrimary = isPrimary;
  }

  public ReplicationActor.State getState() {
    if (isPrimary)
      return ReplicationActor.State.PRIMARY;

    return ReplicationActor.State.SECONDARY;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("  Actor: " + getActor() + Prop.NL);
    b.append("  ID: " + getId() + Prop.NL);
    b.append("  Voting: " + isVoting() + Prop.NL);
    b.append("  Weight: " + getWeight() + Prop.NL);

    return b.toString();
  }
}
