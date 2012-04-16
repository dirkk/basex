package org.basex.query;

import static org.basex.core.Text.*;

import org.basex.core.*;
import org.basex.query.item.*;
import org.basex.query.util.*;
import org.basex.util.*;
import org.basex.util.list.*;

/**
 * This class indicates exceptions during the parsing or evaluation of a query.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class QueryException extends Exception {
  /** Error QName. */
  private final QNm name;
  /** Error value. */
  private transient Value value = Empty.SEQ;
  /** Error reference. */
  private Err err;
  /** File reference. */
  private String file;
  /** Code suggestions. */
  private StringList suggest;
  /** Error line and column. */
  private int[] lineCol;
  /** Marked error column. */
  private int markedCol;

  /**
   * Constructor, specifying a simple error message. {@link Err#JAVACALL} will be set
   * as error code.
   * @param msg error message
   */
  public QueryException(final String msg) {
    this(null, Err.JAVACALL, msg);
  }

  /**
   * Default constructor.
   * @param ii input info
   * @param er error reference
   * @param ext error extension
   */
  public QueryException(final InputInfo ii, final Err er, final Object... ext) {
    this(ii, er.qname(), er.desc, ext);
    err = er;
  }

  /**
   * Constructor, specifying the error code and message as string.
   * @param ii input info
   * @param errc error code
   * @param msg error message
   * @param ext error extension
   */
  public QueryException(final InputInfo ii, final QNm errc, final String msg,
      final Object... ext) {

    super(BaseXException.message(msg, ext));
    name = errc;
    if(ii != null) info(ii);
  }

  /**
   * Returns the error column.
   * @return error column
   */
  public int col() {
    return lineCol == null ? 0 : lineCol[1];
  }

  /**
   * Returns the marked error column.
   * @return marked error column
   */
  public int markedCol() {
    return markedCol;
  }

  /**
   * Returns the error line.
   * @return error line
   */
  public int line() {
    return lineCol == null ? 0 : lineCol[0];
  }

  /**
   * Returns the file.
   * @return error line
   */
  public String file() {
    return file;
  }

  /**
   * Returns suggestions for code suggestions.
   * @return suggestions
   */
  public StringList suggest() {
    return suggest == null ? new StringList() : suggest;
  }

  /**
   * Sets code suggestions.
   * @param qp query parser
   * @param sug code suggestions
   * @return self reference
   */
  public QueryException suggest(final InputParser qp, final StringList sug) {
    suggest = sug;
    pos(qp);
    return this;
  }

  /**
   * Sets input info.
   * @param ii input info
   * @return self reference
   */
  public QueryException info(final InputInfo ii) {
    file = ii.file;
    lineCol = ii.lineCol();
    return this;
  }

  /**
   * Sets the error value.
   * @param v error value
   * @return self reference
   */
  public QueryException value(final Value v) {
    value = v;
    return this;
  }

  /**
   * Finds line and column for the specified query parser.
   * @param parser parser
   */
  void pos(final InputParser parser) {
    markedCol = parser.im;
    // check if line/column information has already been added
    if(lineCol != null) return;

    file = parser.file;
    lineCol = InputInfo.lineCol(parser.input, Math.min(parser.im, parser.il));
  }

  /**
   * Returns the error code.
   * @return error code
   */
  public QNm qname() {
    return name;
  }

  /**
   * Returns the error code.
   * @return error code
   */
  public Err err() {
    return err;
  }

  /**
   * Returns the error value.
   * @return error value
   */
  public Value value() {
    return value;
  }

  @Override
  public String getLocalizedMessage() {
    return super.getMessage();
  }

  @Override
  public String getMessage() {
    final TokenBuilder tb = new TokenBuilder();
    if(lineCol != null) {
      tb.add(STOPPED_AT).add(' ').addExt(LINE_X, lineCol[0]);
      if(lineCol[1] != 0) tb.add(QueryText.SEP).addExt(COLUMN_X, lineCol[1]);
      if(file != null) tb.add(' ').addExt(IN_FILE_X, file);
      tb.add(COL).add(NL);
    }
    final byte[] code = name.local();
    if(code.length != 0) tb.add('[').add(code).add("] ");
    return tb.add(getLocalizedMessage()).toString();
  }
}
