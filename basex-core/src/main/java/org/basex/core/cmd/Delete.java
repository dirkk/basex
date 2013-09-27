package org.basex.core.cmd;

import org.basex.core.Perm;
import org.basex.data.Data;
import org.basex.data.atomic.AtomicUpdateList;
import org.basex.io.IOFile;
import org.basex.util.list.IntList;
import org.basex.util.list.TokenList;

import static org.basex.core.Text.DB_PINNED_X;
import static org.basex.core.Text.RES_DELETED_X_X;

/**
 * Evaluates the 'delete' command and deletes resources from a database.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class Delete extends ACreate {
  /**
   * Default constructor.
   * @param target target to delete
   */
  public Delete(final String target) {
    super(Perm.WRITE, true, target);
  }

  @Override
  protected boolean run() {
    final Data data = context.data();
    final String target = args[0];

    // start update
    if(!data.startUpdate()) return error(DB_PINNED_X, data.meta.name);

    // delete all documents
    final IntList docs = data.resources.docs(target);
    final AtomicUpdateList atomics = new AtomicUpdateList(data);
    for(int d = docs.size() - 1; d >= 0; d--)
      atomics.addDelete(docs.get(d));
    atomics.execute(false);
    context.update();

    // delete binaries
    final TokenList bins = data.resources.binaries(target);
    delete(data, target);

    context.triggers.afterDelete(target);

    // finish update
    data.finishUpdate();

    // return info message
    return info(RES_DELETED_X_X, docs.size() + bins.size(), perf);
  }


  /**
   * Deletes the specified resources.
   * @param data data reference
   * @param res resource to be deleted
   */
  public static void delete(final Data data, final String res) {
    if(data.inMemory()) return;
    final IOFile file = data.meta.binary(res);
    if(file != null) file.delete();
  }
}
