package org.basex.trigger;

import org.basex.core.BaseXException;
import org.basex.query.value.node.DBNode;

import java.util.*;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class TriggerManager implements DocumentTrigger, DatabaseTrigger{
  private Map<String, List<DocumentTrigger>> docTriggers;
  private Map<String, List<DatabaseTrigger>> dbTriggers;

  public TriggerManager() {
    docTriggers = new HashMap<String, List<DocumentTrigger>>();
    dbTriggers = new HashMap<String, List<DatabaseTrigger>>();
  }

  /**
   * Register a document trigger for all documents.
   *
   * @param t trigger
   * @throws BaseXException
   */
  public void register(final Trigger t) throws BaseXException {
    register(t, "*");
  }

  /**
   * Register a trigger for a specific path. '*' matches all documents.
   * @param t trigger
   * @param path document path, wildcard supported
   * @throws BaseXException
   */
  public void register(final Trigger t, final String path) throws BaseXException {
    if (t instanceof DocumentTrigger) {
      registerDocumentTrigger((DocumentTrigger) t, path);
    }

    if(t instanceof DatabaseTrigger) {
      registerDatabaseTrigger((DatabaseTrigger) t, path);
    }
  }

  /**
   * Register a document trigger for a specific path. '*' matches all documents.
   * @param t trigger
   * @param path document path, wildcard supported
   * @throws BaseXException
   */
  public void registerDocumentTrigger(final DocumentTrigger t, final String path) throws BaseXException {
    if (docTriggers.containsKey(path)) {
      docTriggers.get(path).add(t);
    } else {
      LinkedList l = new LinkedList<DocumentTrigger>();
      l.add(t);
      docTriggers.put(path, l);
    }
  }

  /**
   * Register a database trigger for a specific path. '*' matches all documents.
   * @param t trigger
   * @param name database name, wildcard supported
   * @throws BaseXException
   */
  public void registerDatabaseTrigger(final DatabaseTrigger t, final String name) throws BaseXException {
    if (dbTriggers.containsKey(name)) {
      dbTriggers.get(name).add(t);
    } else {
      LinkedList l = new LinkedList<DatabaseTrigger>();
      l.add(t);
      dbTriggers.put(name, l);
    }
  }

  /**
   * Unregister a trigger from the database context. Does nothing, if trigger did not
   * exist.
   *
   * @param tClass trigger class to remove
   */
  public void unregister(final Class tClass) {
      for (List<DocumentTrigger> l : docTriggers.values()) {
        for (DocumentTrigger t : l) {
          if (tClass.isInstance(t)) {
            l.remove(t);
          }
        }
      }

      for (List<DatabaseTrigger> l : dbTriggers.values()) {
        for (DatabaseTrigger t : l) {
          if (tClass.isInstance(t)) {
            l.remove(t);
          }
        }
      }
  }

  /**
   * Returns a list of document triggers. Returns an empty list of no document triggers for
   * this key are present.
   *
   * @param search trigger key
   * @return list of document triggers
   */
  private List<DocumentTrigger> getDocumentTriggers(final String search) {
    if (docTriggers.containsKey(search)) {
      return docTriggers.get("*");
    }

    return new LinkedList<DocumentTrigger>();
  }

  /**
   * Returns a list of database triggers. Returns an empty list of no database triggers for
   * this key are present.
   *
   * @param search trigger key
   * @return list of database triggers
   */
  private List<DatabaseTrigger> getDatabaseTriggers(final String search) {
    if (dbTriggers.containsKey(search)) {
      return dbTriggers.get("*");
    }

    return new LinkedList<DatabaseTrigger>();
  }

  @Override
  public void afterCreateDb(final String name) {
    for (DatabaseTrigger t : getDatabaseTriggers("*")) {
      t.afterCreateDb(name);
    }
  }

  @Override
  public void afterAlter(final String source, final String target) {
    for(DatabaseTrigger t : getDatabaseTriggers("*")) {
      t.afterAlter(source, target);
    }
  }

  @Override
  public void afterDrop(final String name) {
    for(DatabaseTrigger t : getDatabaseTriggers("*")) {
      t.afterDrop(name);
    }
  }

  @Override
  public void afterCopy(final String source, final String target) {
    for(DatabaseTrigger t : getDatabaseTriggers("*")) {
      t.afterCopy(source, target);
    }
  }

  @Override
  public void afterAdd(final DBNode node) {
    for(DocumentTrigger t : getDocumentTriggers("*")) {
      t.afterAdd(node);
    }
  }

  @Override
  public void afterUpdate(final DBNode node) {
    for(DocumentTrigger t : getDocumentTriggers("*")) {
      t.afterUpdate(node);
    }
  }

  @Override
  public void afterDelete(final String path, final String database) {
    for(DocumentTrigger t : getDocumentTriggers("*")) {
      t.afterDelete(path, database);
    }
  }

  @Override
  public void afterRename(final String oldPath, final String newPath, final String database) {
    for(DocumentTrigger t : getDocumentTriggers("*")) {
      t.afterRename(oldPath, newPath, database);
    }
  }

}
