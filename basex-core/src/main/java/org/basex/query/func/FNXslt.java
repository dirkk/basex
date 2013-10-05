package org.basex.query.func;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;
import static org.basex.util.Reflect.*;
import static org.basex.util.Token.*;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import javax.xml.transform.*;
import javax.xml.transform.stream.*;

import org.basex.core.*;
import org.basex.io.*;
import org.basex.io.out.*;
import org.basex.query.*;
import org.basex.query.expr.*;
import org.basex.query.value.item.*;
import org.basex.query.value.node.*;
import org.basex.util.*;

/**
 * Functions for performing XSLT transformations.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class FNXslt extends StandardFunc {
  /** Element: parameters. */
  private static final QNm Q_PARAMETERS = QNm.get("parameters", XSLTURI);

  /** XSLT implementations. */
  private static final String[] IMPL = {
    "", "Java", "1.0",
    "net.sf.saxon.TransformerFactoryImpl", "Saxon HE", "2.0",
    "com.saxonica.config.ProfessionalTransformerFactory", "Saxon PE", "2.0",
    "com.saxonica.config.EnterpriseTransformerFactory", "Saxon EE", "2.0"
  };
  /** Implementation offset. */
  private static final int OFFSET;

  static {
    final String fac = TransformerFactory.class.getName();
    final String impl = System.getProperty(fac);
    // system property has already been set
    if(System.getProperty(fac) != null) {
      // modify processor and version
      IMPL[1] = impl;
      IMPL[2] = "Unknown";
      OFFSET = 0;
    } else {
      // search for existing processors
      int s = IMPL.length - 3;
      while(s != 0 && find(IMPL[s]) == null) s -= 3;
      OFFSET = s;
      // set processor, or use default processor
      if(s != 0) Options.setSystem(fac, IMPL[s]);
    }
  }

  /**
   * Returns details on the XSLT implementation.
   * @param name name flag
   * @return string
   */
  static String get(final boolean name) {
    return IMPL[OFFSET + (name ? 1 : 2)];
  }

  /**
   * Constructor.
   * @param ii input info
   * @param f function definition
   * @param e arguments
   */
  public FNXslt(final InputInfo ii, final Function f, final Expr... e) {
    super(ii, f, e);
  }

  @Override
  public Item item(final QueryContext ctx, final InputInfo ii) throws QueryException {
    switch(sig) {
      case _XSLT_PROCESSOR:      return Str.get(get(true));
      case _XSLT_VERSION:        return Str.get(get(false));
      case _XSLT_TRANSFORM:      return transform(ctx, true);
      case _XSLT_TRANSFORM_TEXT: return transform(ctx, false);
      default:                   return super.item(ctx, ii);
    }
  }

  /**
   * Performs an XSL transformation.
   * @param ctx query context
   * @param node return result as node
   * @return item
   * @throws QueryException query exception
   */
  private Item transform(final QueryContext ctx, final boolean node)
      throws QueryException {

    checkCreate(ctx);
    final IO in = read(checkItem(expr[0], ctx));
    final IO xsl = read(checkItem(expr[1], ctx));

    final Options opts = new Options();
    if(expr.length > 2) {
      new FuncOptions(Q_PARAMETERS, info).parse(expr[2].item(ctx, info), opts);
    }

    final PrintStream tmp = System.err;
    final ArrayOutput ao = new ArrayOutput();
    try {
      System.setErr(new PrintStream(ao));
      final byte[] result = transform(in, xsl, opts.free());
      return node ? new DBNode(new IOContent(result), ctx.context.options) : Str.get(result);
    } catch(final IOException ex) {
      System.setErr(tmp);
      throw IOERR.thrw(info, ex);
    } catch(final TransformerException ex) {
      System.setErr(tmp);
      throw BXSL_ERROR.thrw(info, Token.trim(Token.utf8(ao.toArray(), Prop.ENCODING)));
    } finally {
      System.setErr(tmp);
    }
  }

  /**
   * Returns an input reference (possibly cached) to the specified input.
   * @param it item to be evaluated
   * @return item
   * @throws QueryException query exception
   */
  private IO read(final Item it) throws QueryException {
    if(it.type.isNode()) {
      final IO io = new IOContent(it.serialize().toArray());
      io.name(string(((ANode) it).baseURI()));
      return io;
    }

    if(it.type.isStringOrUntyped()) {
      final String path = string(it.string(info));
      final IO io = IO.get(path);
      if(!io.exists()) WHICHRES.thrw(info, path);
      return io;
    }
    throw STRNODTYPE.thrw(info, this, it.type);
  }

  /**
   * Uses Java's XSLT implementation to perform an XSL transformation.
   * @param in input
   * @param xsl style sheet
   * @param par parameters
   * @return transformed result
   * @throws TransformerException transformer exception
   */
  private static byte[] transform(final IO in, final IO xsl, final HashMap<String, String> par)
      throws TransformerException {

    // create transformer
    final TransformerFactory tc = TransformerFactory.newInstance();
    final Transformer tr =  tc.newTransformer(xsl.streamSource());

    // bind parameters
    for(final Entry<String, String> entry : par.entrySet())
      tr.setParameter(entry.getKey(), entry.getValue());

    // do transformation and return result
    final ArrayOutput ao = new ArrayOutput();
    tr.transform(in.streamSource(), new StreamResult(ao));
    return ao.toArray();
  }
}
