package org.basex.dist;

import java.util.*;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.*;

public class DistributedQuery extends DistributedQuerySingle {
  /** Peers where this query is send to. */
  public List<ClusterPeer> peers;
  /** Overall combined result. */
  @SuppressWarnings("hiding")
  public String result;
  /** Distributed queries which already have been returned. */
  private List<DistributedQuerySingle> returnedQueries;
  /** Number of outstanding results. */
  private Integer outstandingResults;

  /**
   * Default Constructor.
   * @param q Query.
   * @param s Sequence number.
   */
  public DistributedQuery(final String q, final int s) {
    super(q, s);

    peers = new LinkedList<ClusterPeer>();
    result = null;
    outstandingResults = 0;
    returnedQueries = new LinkedList<DistributedQuerySingle>();
  }

  /**
   * All peers have send their result. So this function now merges
   * all the different results into one consistent.
   */
  private void mergeResult() {
    Iterator<DistributedQuerySingle> it = returnedQueries.iterator();
    while (it.hasNext()) {
      DistributedQuerySingle q = it.next();
      Util.println(q.result);
    }
  }

  /**
   * Add a new peer to the list of peers to execute this query.
   * @param cp Peer to add.
   */
  public void addPeer(final ClusterPeer cp) {
    peers.add(cp);
    synchronized(outstandingResults) {
      ++outstandingResults;
    }
  }

  /**
   * Notify of an incoming result from some peer.
   * @param q The returned distributed query.
   */
  public void newResult(final DistributedQuerySingle q) {
    returnedQueries.add(q);
    synchronized (outstandingResults) {
      --outstandingResults;
      if (outstandingResults <= 0) {
        mergeResult();
      }
    }
  }
}
