package org.basex.dist;

import java.io.*;
import java.util.*;

import org.basex.core.*;
import org.basex.io.out.*;
import org.basex.util.*;

/**
 * A distributed query. Contains the results and queries send to the
 * different connected peers within the network.
 * @author Dirk Kirsten
 */
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
  /** Output stream currently used. */
  private PrintOutput out;

  /**
   * Default Constructor.
   * @param q Query to execute
   * @param s Sequence number
   * @param o output stream to output the result
   */
  public DistributedQuery(final String q, final int s, final PrintOutput o) {
    super(q, s);

    peers = new LinkedList<ClusterPeer>();
    result = null;
    outstandingResults = 0;
    returnedQueries = new LinkedList<DistributedQuerySingle>();
    out = o;
  }

  /**
   * All peers have send their result. So this function now merges
   * all the different results into one consistent.
   * @throws IOException I/O error
   */
  private void mergeResult() throws IOException {
    Iterator<DistributedQuerySingle> it = returnedQueries.iterator();
    result = new String();
    while (it.hasNext()) {
      DistributedQuerySingle q = it.next();
      if (q.result == null)
        throw new BaseXException("Invalid result for one peer.");
      result += q.result;
    }

    out.write(Token.token("test"));
    out.write(Token.token(result));
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
        try {
          mergeResult();
        } catch(IOException ex) {
          Util.debug(ex);
        }
      }
    }
  }
}
