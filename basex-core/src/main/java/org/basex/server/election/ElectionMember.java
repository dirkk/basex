package org.basex.server.election;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ElectionMember implements Serializable, Comparable {
  /** Election actor reference. */
  private final ActorRef actor;
  /** Process number. Has to be unique within an election system. */
  private final Comparable processNumber;

  public ElectionMember(final ActorRef actor, final Comparable processNumber) {
    this.actor = actor;
    this.processNumber = processNumber;
  }

  public ActorRef getActor() {
    return actor;
  }

  public Comparable getProcessNumber() {
    return processNumber;
  }

  @Override
  public int compareTo(Object o) {
    return processNumber.compareTo(o);
  }
}
