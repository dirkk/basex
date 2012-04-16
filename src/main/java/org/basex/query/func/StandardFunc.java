package org.basex.query.func;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;
import static org.basex.util.Token.*;

import java.io.*;

import org.basex.data.*;
import org.basex.io.serial.*;
import org.basex.query.*;
import org.basex.query.expr.*;
import org.basex.query.item.*;
import org.basex.util.*;

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
  StandardFunc(final InputInfo ii, final Function s, final Expr... args) {
    super(ii, args);
    sig = s;
    type = sig.ret;
  }

  @Override
  public final Expr comp(final QueryContext ctx) throws QueryException {
    // compile all arguments
    super.comp(ctx);
    // skip context-based or non-deterministic functions, and non-values
    if(uses(Use.CTX) || uses(Use.NDT) || !allAreValues())
      return optPre(cmp(ctx), ctx);
    // pre-evaluate function
    return optPre(sig.ret.zeroOrOne() ? item(ctx, info) : value(ctx), ctx);
  }

  /**
   * Performs function specific compilations.
   * @param ctx query context
   * @return evaluated item
   * @throws QueryException query exception
   */
  @SuppressWarnings("unused")
  Expr cmp(final QueryContext ctx) throws QueryException {
    return this;
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
    return ip.isNode() ? ip == NodeType.PI || ip == NodeType.COM ?
        Str.get(it.string(ii)) : new Atm(it.string(ii)) : it;
  }

  @Override
  public final boolean isFunction(final Function f) {
    return sig == f;
  }

  @Override
  public final String description() {
    return sig.toString();
  }

  @Override
  public final void plan(final Serializer ser) throws IOException {
    ser.openElement(this, NAM, token(sig.desc));
    for(final Expr arg : expr) arg.plan(ser);
    ser.closeElement();
  }

  @Override
  public final String toString() {
    final String desc = sig.toString();
    return new TokenBuilder().add(desc.substring(0,
        desc.indexOf('(') + 1)).addSep(expr, SEP).add(PAR2).toString();
  }

  /**
   * Returns the data instance for the specified argument.
   * @param i index of argument
   * @param ctx query context
   * @return data instance
   * @throws QueryException query exception
   */
  Data data(final int i, final QueryContext ctx) throws QueryException {
    final Item it = checkNoEmpty(expr[i].item(ctx, info));
    final Type ip = it.type;
    if(ip.isNode()) return checkDBNode(it).data;
    if(ip.isString())  {
      final String name = string(it.string(info));
      if(!MetaData.validName(name, false)) INVDB.thrw(info, name);
      return ctx.resource.data(name, info);
    }
    throw STRNODTYPE.thrw(info, this, ip);
  }
}
