package org.basex.core.cmd;

import org.basex.core.Command;
import org.basex.core.Databases;
import org.basex.core.LockResult;
import org.basex.core.Perm;
import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.util.Util;
import org.basex.util.list.StringList;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.basex.core.Text.*;

/**
 * Evaluates the 'copy' command and creates a copy of a database.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Andreas Weiler
 */
public final class Copy extends Command {
  /** Pattern to exclude locking files from database transfer operations. */
  private static final Pattern FILES = Pattern.compile(".{3,5}" + IO.BASEXSUFFIX);
  /** Counter for outstanding files. */
  private int of;
  /** Counter of total files. */
  private int tf;

  /**
   * Default constructor.
   * @param db db name
   * @param newdb new db name
   */
  public Copy(final String db, final String newdb) {
    super(Perm.CREATE, db, newdb);
  }

  @Override
  protected boolean run() {
    final String src = args[0];
    final String trg = args[1];
    // check if names are valid
    if(!Databases.validName(src)) return error(NAME_INVALID_X, src);
    if(!Databases.validName(trg)) return error(NAME_INVALID_X, trg);

    // source database does not exist
    if(!mprop.dbexists(src)) return error(DB_NOT_FOUND_X, src);
    // target database already exists
    if(mprop.dbexists(trg)) return error(DB_EXISTS_X, trg);

    // try to copy database
    return copy(src, trg) ? info(DB_COPIED_X, src, perf) : error(DB_NOT_COPIED_X, src);
  }

  /**
   * Copies the specified database.
   * @param source name of the database
   * @param target new database name
   * @return success flag
   */
  private boolean copy(final String source, final String target) {
    final IOFile src = mprop.dbpath(source);
    final IOFile trg = mprop.dbpath(target);

    // return false if source cannot be opened, or target cannot be created
    final StringList files = src.descendants();
    tf = files.size();
    try {
      for(final String file : files) {
        if(FILES.matcher(file).matches()) {
          new IOFile(src, file).copyTo(new IOFile(trg, file));
        }
        of++;
      }

      context.triggers.afterCopy(source, target);
      return true;
    } catch(final IOException ex) {
      // drop new database if error occurred
      Util.debug(ex);
      DropDB.drop(target, context);
      return false;
    }
  }

  @Override
  public void databases(final LockResult lr) {
    lr.read.add(args[0]);
    lr.write.add(args[1]);
  }

  @Override
  protected String tit() {
    return COPY;
  }

  @Override
  public boolean supportsProg() {
    return true;
  }

  @Override
  protected double prog() {
    return (double) of / tf;
  }
}
