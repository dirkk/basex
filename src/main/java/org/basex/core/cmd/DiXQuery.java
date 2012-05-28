package org.basex.core.cmd;

import org.basex.core.*;

/**
 * Evaluates the 'xquery' command and processes an XQuery request in a distributed
 * BaseX network.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class DiXQuery extends AQuery {
  /**
   * Default constructor.
   * @param query query to evaluate
   */
  public DiXQuery(final String query) {
    super(Perm.NONE, false, query);
  }

  /**
   * Evaluates the specified query in a distributed fashion.
   * @param query query
   * @return success flag
   */
  boolean dixquery(final String query) {
    context.nNode.executeXQuery(query);

    return true;
  }

  @Override
  protected boolean run() {
    if (context.nNode == null)
      return false;

    return dixquery(args[0]);
  }
}
