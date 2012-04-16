package org.basex.core;

import static org.basex.core.Text.*;

import java.util.*;

import org.basex.data.*;
import org.basex.util.*;

/**
 * This class organizes all currently opened database.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class Datas {
  /** List of data references. */
  private final ArrayList<Data> list = new ArrayList<Data>();

  /**
   * Pins and returns an existing data reference for the specified database, or
   * returns {@code null}.
   * @param db name of the database
   * @return data reference
   */
  synchronized Data pin(final String db) {
    for(final Data d : list) {
      if(d.meta.name.equals(db)) {
        d.pins++;
        return d;
      }
    }
    return null;
  }

  /**
   * Unpins a data reference.
   * @param data data reference
   * @return true if reference was removed from the pool
   */
  synchronized boolean unpin(final Data data) {
    for(final Data d : list) {
      if(d == data) {
        final boolean close = --d.pins == 0;
        if(close) list.remove(d);
        return close;
      }
    }
    return false;
  }

  /**
   * Checks if the specified database is pinned.
   * @param db name of the database
   * @return result of check
   */
  synchronized boolean pinned(final String db) {
    for(final Data d : list) if(d.meta.name.equals(db)) return true;
    return false;
  }

  /**
   * Adds a data reference to the pool.
   * @param d data reference
   */
  synchronized void add(final Data d) {
    list.add(d);
  }

  /**
   * Returns the number of opened databases.
   * @return number of databases
   */
  public synchronized int size() {
    return list.size();
  }

  /**
   * Returns information on the opened database instances.
   * @return data reference
   */
  public synchronized String info() {
    final TokenBuilder tb = new TokenBuilder();
    tb.addExt(OPENED_DB_X, list.size());
    tb.add(!list.isEmpty() ? COL : DOT);
    for(final Data d : list) {
      tb.add(NL + LI + d.meta.name + " (" + d.pins + "x)");
    }
    return tb.toString();
  }

  /**
   * Closes all data references.
   */
  synchronized void close() {
    for(final Data d : list) d.close();
    list.clear();
  }

  /**
   * Returns the number of pins for the specified database,
   * or {@code 0} if the database is not opened.
   * @param db name of the database
   * @return number of references
   */
  public synchronized int pins(final String db) {
    for(final Data d : list) {
      if(d.meta.name.equals(db)) return d.pins;
    }
    return 0;
  }
}
