package org.basex.query.func;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;
import static org.basex.util.Token.*;

import java.io.*;
import java.nio.charset.*;
import java.util.*;

import org.basex.core.*;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.query.*;
import org.basex.query.expr.*;
import org.basex.query.iter.*;
import org.basex.query.util.*;
import org.basex.query.value.*;
import org.basex.query.value.item.*;
import org.basex.query.value.map.Map;
import org.basex.query.value.node.*;
import org.basex.query.value.type.*;
import org.basex.query.var.*;
import org.basex.util.*;
import org.basex.util.hash.*;

/**
 * Standard (built-in) functions.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public abstract class StandardFunc extends Arr {
  /** Function signature. */
  Function sig;

  /**
   * Constructor.
   * @param ii input info
   * @param s function definition
   * @param args arguments
   */
  protected StandardFunc(final InputInfo ii, final Function s, final Expr... args) {
    super(ii, args);
    sig = s;
    type = sig.ret;
  }

  @Override
  public final Expr compile(final QueryContext ctx, final VarScope scp)
      throws QueryException {
    // compile all arguments
    super.compile(ctx, scp);
    return optimize(ctx, scp);
  }

  @Override
  public final Expr optimize(final QueryContext ctx, final VarScope scp)
      throws QueryException {
    // skip context-based or non-deterministic functions, and non-values
    return optPre(has(Flag.CTX) || has(Flag.NDT) || !allAreValues() ? opt(ctx) :
      sig.ret.zeroOrOne() ? item(ctx, info) : value(ctx), ctx);
  }

  /**
   * Performs function specific optimizations.
   * @param ctx query context
   * @return evaluated item
   * @throws QueryException query exception
   */
  @SuppressWarnings("unused")
  protected Expr opt(final QueryContext ctx) throws QueryException {
    return this;
  }

  @Override
  public final StandardFunc copy(final QueryContext ctx, final VarScope scp,
      final IntObjMap<Var> vs) {
    final int es = expr.length;
    final Expr[] arg = new Expr[es];
    for(int e = 0; e < es; e++) arg[e] = expr[e].copy(ctx, scp, vs);
    return sig.get(info, arg);
  }

  /**
   * Atomizes the specified item.
   * @param it input item
   * @param ii input info
   * @return atomized item
   * @throws QueryException query exception
   */
  public static final Item atom(final Item it, final InputInfo ii) throws QueryException {
    final Type ip = it.type;
    return it instanceof ANode ? ip == NodeType.PI || ip == NodeType.COM ?
        Str.get(it.string(ii)) : new Atm(it.string(ii)) : it.materialize(ii);
  }

  @Override
  public boolean has(final Flag flag) {
    return sig.has(flag) || flag != Flag.X30 && super.has(flag);
  }

  @Override
  public final boolean isFunction(final Function f) {
    return sig == f;
  }

  @Override
  public final boolean isVacuous() {
    return !has(Flag.UPD) && type.eq(SeqType.EMP);
  }

  @Override
  public final String description() {
    return sig.toString();
  }

  @Override
  public final void plan(final FElem plan) {
    addPlan(plan, planElem(NAM, sig.desc), expr);
  }

  @Override
  public final String toString() {
    final String desc = sig.toString();
    return new TokenBuilder(desc.substring(0,
        desc.indexOf('(') + 1)).addSep(expr, SEP).add(PAR2).toString();
  }

  /**
   * Returns a data instance for the first string argument of the function.
   * This method assumes that the function has at least one argument.
   * @param ctx query context
   * @return data instance
   * @throws QueryException query exception
   */
  protected final Data checkData(final QueryContext ctx) throws QueryException {
    final String name = string(checkStr(expr[0], ctx));
    if(!Databases.validName(name)) INVDB.thrw(info, name);
    return ctx.resource.data(name, info);
  }

  /**
   * Converts the specified argument to a file instance.
   * @param i argument index
   * @param ctx query context
   * @return file instance
   * @throws QueryException query exception
   */
  protected File checkFile(final int i, final QueryContext ctx) throws QueryException {
    if(i >= expr.length) return null;
    final String file = string(checkStr(expr[i], ctx));
    return (IOUrl.isFileURL(file) ? IOFile.get(file) : new IOFile(file)).file();
  }

  /**
   * Checks if the specified database can be detected for locking, i.e., if the
   * first argument of the tested function is a static string.
   * This method assumes that the function has at least one argument.
   * @param visitor visitor
   * @return result of check
   */
  protected final boolean dataLock(final ASTVisitor visitor) {
    return visitor.lock(expr[0] instanceof Str ? string(((Str) expr[0]).string()) : null);
  }

  /**
   * Returns a normalized encoding representation.
   * @param i index of encoding argument
   * @param err error for invalid encoding
   * @param ctx query context
   * @return text entry
   * @throws QueryException query exception
   */
  protected final String encoding(final int i, final Err err, final QueryContext ctx)
      throws QueryException {

    if(i >= expr.length) return null;
    final String enc = string(checkStr(expr[i], ctx));
    try {
      if(Charset.isSupported(enc)) return normEncoding(enc);
    } catch(final IllegalArgumentException e) {
      /* character set is invalid or unknown (e.g. empty string) */
    }
    throw err.thrw(info, enc);
  }

  /**
   * Converts the specified dateTime to milliseconds.
   * @param e expression
   * @param ctx query context
   * @return resulting value
   * @throws QueryException query exception
   */
  protected final long dateTimeToMs(final Expr e, final QueryContext ctx)
      throws QueryException {

    final Dtm dtm = (Dtm) checkType(checkItem(e, ctx), AtomType.DTM);
    if(dtm.yea() > 292278993) INTRANGE.thrw(info, dtm);
    return dtm.toJava().toGregorianCalendar().getTimeInMillis();
  }

  /**
   * Returns all keys and values of the specified binding argument.
   * @param i index of argument
   * @param ctx query context
   * @return resulting map
   * @throws QueryException query exception
   */
  protected final HashMap<String, Value> bindings(final int i, final QueryContext ctx)
      throws QueryException {

    final HashMap<String, Value> hm = new HashMap<String, Value>();
    final int es = expr.length;
    if(i < es) {
      final Map map = checkMap(expr[i].item(ctx, info));
      for(final Item it : map.keys()) {
        final byte[] key;
        if(it instanceof Str) {
          key = it.string(null);
        } else {
          final QNm qnm = (QNm) checkType(it, AtomType.QNM);
          final TokenBuilder tb = new TokenBuilder();
          if(qnm.uri() != null) tb.add('{').add(qnm.uri()).add('}');
          key = tb.add(qnm.local()).finish();
        }
        hm.put(string(key), map.get(it, info));
      }
    }
    return hm;
  }

  /**
   * Caches and materializes all items of the specified iterator.
   * @param ir iterator
   * @param vb value builder
   * @param ctx query context
   * @throws QueryException query exception
   */
  protected final void cache(final Iter ir, final ValueBuilder vb, final QueryContext ctx)
      throws QueryException {

    for(Item it; (it = ir.next()) != null;) {
      final Data d = it.data();
      if(d != null && !d.inMemory()) {
        it = ((ANode) it).dbCopy(ctx.context.prop);
      } else if(it instanceof FItem) {
        FIVALUE.thrw(info, it);
      }
      vb.add(it.materialize(info));
    }
  }

  /**
   * Compares several signatures for equality.
   * @param sig signature to be found
   * @param sigs signatures to be compared
   * @return result of check
   */
  protected static boolean oneOf(final Function sig, final Function... sigs) {
    for(final Function s : sigs) if(sig == s) return true;
    return false;
  }
}
