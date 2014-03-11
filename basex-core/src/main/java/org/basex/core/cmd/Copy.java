package org.basex.core.cmd;

import static org.basex.core.Text.*;

import java.io.*;
import java.util.regex.*;

import org.basex.core.*;
import org.basex.io.*;
import org.basex.util.*;
import org.basex.util.list.*;

/**
 * Evaluates the 'copy' command and creates a copy of a database.
 *
 * @author BaseX Team 2005-14, BSD License
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
    if(!goptions.dbexists(src)) return error(DB_NOT_FOUND_X, src);
    // target database already exists
    if(goptions.dbexists(trg)) return error(DB_EXISTS_X, trg);

    // try to copy database
    try {
      copy(src, trg, context, this);
      return info(DB_COPIED_X, src, perf);
    } catch(final IOException ex) {
      return error(DB_NOT_COPIED_X, src);
    }
  }

  /**
   * Copies the specified database.
   * @param source name of the database
   * @param target new database name
   * @param context database context
   * @param cmd calling command
   * @throws IOException I/O exception
   */
  public static void copy(final String source, final String target, final Context context,
      final Copy cmd) throws IOException {

    final GlobalOptions goptions = context.globalopts;
    final IOFile src = goptions.dbpath(source);
    final IOFile trg = goptions.dbpath(target);

    // drop target database
    DropDB.drop(target, context);

    // return false if source cannot be opened, or target cannot be created
    final StringList files = src.descendants();
    if(cmd != null) cmd.tf = files.size();
    try {
      for(final String file : files) {
        if(FILES.matcher(file).matches()) new IOFile(src, file).copyTo(new IOFile(trg, file));
        if(cmd != null) cmd.of++;
      }

      context.triggers.afterCopy(source, target);
    } catch(final IOException ex) {
      // drop new database if error occurred
      Util.debug(ex);
      DropDB.drop(target, context);
      throw ex;
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
