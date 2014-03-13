package org.basex.server.replication;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.basex.server.replication.ReplicaSet.ReplicaSetState;

/**
 * All messages related to the initial connection of a new member in a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface ConnectionMessages {
  /**
   * Send from a newly connected cluster member to the primary.
   * Gives information about the member itself (id, weight, voting
   * or non-voting member) and about all databases and their latest timestamp.
   */
  public class ConnectionStart implements Serializable {
    /** ID. */
    private final String id;
    /** Voting or non-voting member in a primary election. */
    private final boolean isVoting;
    /** Weight. Is used to determine a new primary. */
    private final int weight;
    /** Latest write timestamp for each database. */
    private final Map<String, Integer> databases;

    public ConnectionStart(final String id, final boolean isVoting, final int weight,
                           final Map<String, Integer> databases) {
      this.id = id;
      this.isVoting = isVoting;
      this.weight = weight;
      this.databases = databases;
    }

    public boolean isVoting() {
      return isVoting;
    }

    public int getWeight() {
      return weight;
    }

    public String getId() {
      return id;
    }

    public Map<String, Integer> getDatabases() {
      return databases;
    }
  }

  /**
   * A database at the new member is outdated, so we synchronize the whole database
   * from the primary to the new member.
   * Send the database name and the UTF-8 encoded data.
   */
  public class SyncDatabase implements Serializable {
    /** Database name. */
    private final String name;
    /** Data. */
    private final byte[] data;

    public SyncDatabase(String name, byte[] data) {
      this.name = name;
      this.data = data;
    }
  }

  /**
   * The synchronization of all databases is finished and the new member
   * is now connected and will get all updates from now on.
   */
  public class SyncFinished implements Serializable {
    /** New member status. */
    private final ReplicaSetState state;
    /** Primary of the replica set. */
    private final Member primary;
    /** List of secondaries. */
    private final List<Member> secondaries;

    public SyncFinished(ReplicaSetState state, Member primary, List<Member> secondaries) {
      this.state = state;
      this.primary = primary;
      this.secondaries = secondaries;
    }

    public ReplicaSetState getState() {
      return state;
    }

    public Member getPrimary() {
      return primary;
    }

    public List<Member> getSecondaries() {
      return secondaries;
    }
  }
}