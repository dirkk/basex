package org.basex.dist;

/**
 * Represents a distributed XQuery.
 *
 * @author Dirk Kirsten
 *
 */
public class DistributedQuerySingle {
  /** Query to execute. */
  public String query;
  /** Sequence number. */
  public int seq;
  /** Processing state of this query. */
  public DistConstants.queryState state;
  /** Result of the query. null if the result is not yet available. */
  public String result;

  /**
   * Default constructor.
   * @param q Query to execute
   * @param s sequence number of this query
   */
  public DistributedQuerySingle(final String q, final int s) {
    query = q;
    seq = s;
    state = DistConstants.queryState.QUEUED;
    result = null;
  }
}
