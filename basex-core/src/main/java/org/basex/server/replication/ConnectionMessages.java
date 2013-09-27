package org.basex.server.replication;

import java.io.Serializable;
import java.util.Map;

/**
 * All messages related to the initial connection of a new member in a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface ConnectionMessages {
  /**
   * Send from primary to newly connected cluster member.
   */
  public class ConnectionStart implements Serializable {  }

  /**
   * Send from a newly connected cluster member to the primary.
   * Gives information about the member itself (id, weight, voting
   * or non-voting member) and about all databases and their latest timestamp.
   */
  public class ConnectionResponse implements Serializable {
    /** ID. */
    private final String id;
    /** Voting or non-voting member in a primary election. */
    private final boolean isVoting;
    /** Weight. Is used to determine a new primary. */
    private final int weight;
    /** Latest write timestamp for each database. */
    private final Map<String, Integer> databases;

    public ConnectionResponse(final String id, final boolean isVoting, final int weight,
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
   * is now connected and will get all updates form now on.
   */
  public class SyncFinished implements Serializable {
    /** New member status. */
    private final ReplicationActor.State state;

    public SyncFinished(ReplicationActor.State state) {
      this.state = state;
    }

    public ReplicationActor.State getState() {
      return state;
    }
  }
}