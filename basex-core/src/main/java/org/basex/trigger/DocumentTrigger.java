package org.basex.trigger;

import org.basex.query.value.node.DBNode;

/**
 * Trigger for document related events.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface DocumentTrigger extends Trigger {
  /**
   * A document was added.
   * @param node document node
   * */
  public void afterAdd(final DBNode node);

  /**
   * A document was updated via Xquery Update.
   * @param node document node
   * */
  public void afterUpdate(final DBNode node);
  /**
   * A document was deleted.
   * @param path former document path
   * */
  public void afterDelete(final String path, final String database);
  /**
   * A document was moved.
   * @param oldPath old document path
   * @param newPath new document path
   * */
  public void afterRename(final String oldPath, final String newPath, final String database);

}
