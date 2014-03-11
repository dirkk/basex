package org.basex.query.up;

import org.basex.data.Data;
import org.basex.data.MemData;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.up.primitives.DataUpdate;
import org.basex.query.up.primitives.NameUpdate;
import org.basex.query.up.primitives.Update;
import org.basex.query.value.node.DBNode;
import org.basex.trigger.TriggerManager;
import org.basex.util.list.StringList;

import java.util.*;

import static org.basex.query.util.Err.BXDB_OPENED;

/**
 * Base class for the different context modifiers. A context modifier aggregates
 * all updates for a specific context.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Lukas Kircher
 */
public abstract class ContextModifier {
  /** Update primitives, aggregated separately for each database. */
  private final Map<Data, DataUpdates> dbUpdates = new HashMap<Data, DataUpdates>();
  /** Update primitives, aggregated separately for each database name. */
  private final Map<String, NameUpdates> nameUpdates = new HashMap<String, NameUpdates>();
  /** Temporary data reference, containing all XML fragments to be inserted. */
  private MemData tmp;

  /**
   * Adds an update primitive to this context modifier.
   * @param up update primitive
   * @param ctx query context
   * @throws QueryException query exception
   */
  abstract void add(final Update up, final QueryContext ctx) throws QueryException;

  /**
   * Adds an update primitive to this context modifier.
   * Will be called by {@link #add(Update, QueryContext)}.
   * @param up update primitive
   * @throws QueryException query exception
   */
  protected final void add(final Update up) throws QueryException {
    if(up instanceof DataUpdate) {
      final DataUpdate dataUp = (DataUpdate) up;
      final Data data = dataUp.data();
      DataUpdates ups = dbUpdates.get(data);
      if(ups == null) {
        ups = new DataUpdates(data);
        dbUpdates.put(data, ups);
      }
      // create temporary mem data instance if not available yet
      if(tmp == null) tmp = new MemData(data.meta.options);
      ups.add(dataUp, tmp);
    } else {
      final NameUpdate nameUp = (NameUpdate) up;
      final String name = nameUp.name();
      NameUpdates ups = nameUpdates.get(name);
      if(ups == null) {
        ups = new NameUpdates();
        nameUpdates.put(name, ups);
      }
      ups.add(nameUp);
    }
  }

  /**
   * Adds all databases to be updated to the specified list.
   * @param db databases
   */
  void databases(final StringList db) {
    for(final Data data : dbUpdates.keySet()) {
      if(!data.inMemory()) db.add(data.meta.name);
    }
    for(final NameUpdates up : nameUpdates.values()) {
      up.databases(db);
    }
  }

  /**
   * Adds all documents belonging to all databases to be updated to the
   * specified list.
   * @return documents
   */
  private List<DBNode> documents() {
    final List<DBNode> docs = new LinkedList<DBNode>();
    for(final DataUpdates du : dbUpdates.values()) {
      final Data d = du.data();

      if(!d.inMemory()) {
        docs.addAll(du.getDocuments());
      }
    }
    
    return docs;
  }

  /**
   * Checks constraints and applies all updates.
   * @throws QueryException query exception
   */
  final void apply(final TriggerManager tr) throws QueryException {
    // prepare updates
    for(final DataUpdates up : dbUpdates.values()) {
      // create temporary mem data instance if not available yet
      if(tmp == null) tmp = new MemData(up.data().meta.options);
      up.prepare(tmp);
    }
    for(final NameUpdates up : nameUpdates.values()) up.prepare();

    // apply initial updates based on database names
    for(final NameUpdates up : nameUpdates.values()) up.apply(true);

    // collect data references to be locked
    final Set<Data> datas = new HashSet<Data>();
    for(final Data data : dbUpdates.keySet()) datas.add(data);

    // try to acquire write locks and keep track of the number of acquired locks in order to
    // release them in case of error. write locks prevent other JVMs from accessing currently
    // updated databases, but they cannot provide perfect safety.
    int i = 0;
    try {
      for(final Data data : datas) {
        if(!data.startUpdate()) throw BXDB_OPENED.get(null, data.meta.name);
        i++;
      }
      // apply node and database update
      for(final DataUpdates c : dbUpdates.values()) {
        c.apply();

        for (DBNode n : c.getDocuments()) {
          tr.afterUpdate(n);
        }
      }
    } finally {
      // remove locks: in case of a crash, remove only already acquired write locks
      for(final Data data : datas) {
        if(i-- > 0) data.finishUpdate();
      }
    }

    // apply remaining updates based on database names
    for(final NameUpdates up : nameUpdates.values()) up.apply(false);
  }

  /**
   * Returns the total number of update operations.
   * @return number of updates
   */
  final int size() {
    int s = 0;
    for(final DataUpdates c : dbUpdates.values()) s += c.size();
    for(final NameUpdates c : nameUpdates.values()) s += c.size();
    return s;
  }
}
