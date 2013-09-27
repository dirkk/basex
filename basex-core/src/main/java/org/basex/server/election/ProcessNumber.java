package org.basex.server.election;

import java.io.Serializable;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ProcessNumber implements Comparable, Serializable {
  /** Weight. Primary comparator. */
  private final Integer weight;
  /** ID. Secondary comparator, if weight is equal. */
  private final String id;

  public ProcessNumber(int weight, String id) {
    this.weight = weight;
    this.id = id;
  }

  public Integer getWeight() {
    return weight;
  }

  public String getId() {
    return id;
  }

  @Override
  public int compareTo(Object o) {
    if (o instanceof ProcessNumber) {
      ProcessNumber other = (ProcessNumber) o;
      return other.getWeight() == getWeight() ? getId().compareTo(other.getId()) : getWeight().compareTo(other.getWeight());
    }

    return 0;
  }

  @Override
  public String toString() {
    return "Weight: " + getWeight() + ", ID: " + getId();
  }
}
