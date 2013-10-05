package org.basex.query.func;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;

import java.io.*;

import org.basex.build.*;
import org.basex.io.*;
import org.basex.query.*;
import org.basex.query.expr.*;
import org.basex.query.value.item.*;
import org.basex.query.value.node.*;
import org.basex.util.*;

/**
 * Functions for converting HTML to XML.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class FNHtml extends StandardFunc {
  /** QName. */
  private static final QNm Q_OPTIONS = QNm.get("options", HTMLURI);

  /**
   * Constructor.
   * @param ii input info
   * @param f function definition
   * @param e arguments
   */
  public FNHtml(final InputInfo ii, final Function f, final Expr... e) {
    super(ii, f, e);
  }

  @Override
  public Item item(final QueryContext ctx, final InputInfo ii) throws QueryException {
    switch(sig) {
      case _HTML_PARSER: return Str.get(HtmlParser.parser());
      case _HTML_PARSE:  return parse(ctx);
      default:           return super.item(ctx, ii);
    }
  }

  /**
   * Converts HTML input to XML and returns a document node.
   * @param ctx query context
   * @return document node
   * @throws QueryException query exception
   */
  private DBNode parse(final QueryContext ctx) throws QueryException {
    final byte[] in = checkStrBin(checkItem(expr[0], ctx));
    final IO io = new IOContent(in);

    // bind parameters
    final Item opt = expr.length > 1 ? expr[1].item(ctx, info) : null;
    final HtmlOptions opts = new HtmlOptions();
    new FuncOptions(Q_OPTIONS, info).parse(opt, opts);

    // convert html
    try {
      return new DBNode(new HtmlParser(io, ctx.context.options, opts));
    } catch(final IOException ex) {
      throw BXHL_IO.thrw(info, ex);
    }
  }
}
