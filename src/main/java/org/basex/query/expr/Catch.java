package org.basex.query.expr;

import static org.basex.query.QueryText.*;
import static org.basex.util.Token.*;

import org.basex.query.*;
import org.basex.query.func.*;
import org.basex.query.path.*;
import org.basex.query.util.*;
import org.basex.query.value.*;
import org.basex.query.value.item.*;
import org.basex.query.value.seq.*;
import org.basex.query.value.type.*;
import org.basex.query.var.*;
import org.basex.util.*;
import org.basex.util.hash.*;

/**
 * Catch clause.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class Catch extends Single {
  /** Prefix wildcard. */
  public static final byte[] WILDCARD = token("*");

  /** Error QNames. */
  public static final QNm[] NAMES = {
    create(E_CODE), create(E_DESCRIPTION), create(E_VALUE), create(E_MODULE),
    create(E_LINE_NUMBER), create(E_COLUM_NUMBER), create(E_ADDITIONAL)
  };
  /** Error types. */
  public static final SeqType[] TYPES = {
    SeqType.QNM, SeqType.STR_ZO, SeqType.ITEM_ZM, SeqType.STR_ZO,
    SeqType.ITR_ZO, SeqType.ITR_ZO, SeqType.ITEM_ZM
  };

  /** Error variables. */
  private final Var[] vars = new Var[NAMES.length];
  /** Supported codes. */
  private final NameTest[] codes;

  /**
   * Constructor.
   * @param ii input info
   * @param c supported error codes
   * @param ctx query context
   * @param scp variable scope
   */
  public Catch(final InputInfo ii, final NameTest[] c, final QueryContext ctx,
      final VarScope scp) {
    super(ii, null);
    codes = c;
    for(int i = 0; i < NAMES.length; i++)
      vars[i] = scp.newLocal(ctx, NAMES[i], TYPES[i], false);
  }

  @Override
  public Catch compile(final QueryContext ctx, final VarScope scp) throws QueryException {
    try {
      expr = expr.compile(ctx, scp);
      type = expr.type();
    } catch(final QueryException qe) {
      expr = FNInfo.error(qe);
    }
    return this;
  }

  /**
   * Returns the value of the caught expression.
   * @param ctx query context
   * @param ex thrown exception
   * @return resulting item
   * @throws QueryException query exception
   */
  Value value(final QueryContext ctx, final QueryException ex) throws QueryException {
    int i = 0;
    final byte[] io = ex.file() == null ? EMPTY : token(ex.file());
    final Value val = ex.value();
    for(final Value v : new Value[] { ex.qname(),
        Str.get(ex.getLocalizedMessage()), val == null ? Empty.SEQ : val,
        Str.get(io), Int.get(ex.line()), Int.get(ex.col()),
        Str.get(ex.getMessage().replaceAll("\r\n?", "\n")) }) {
      ctx.set(vars[i++], v, info);
    }
    return ctx.value(expr);
  }

  @Override
  public Expr copy(final QueryContext ctx, final VarScope scp, final IntObjMap<Var> vs) {
    final Catch ctch = new Catch(info, codes.clone(), ctx, scp);
    for(int i = 0; i < vars.length; i++) vs.put(vars[i].id, ctch.vars[i]);
    ctch.expr = expr.copy(ctx, scp, vs);
    return ctch;
  }

  @Override
  public Catch inline(final QueryContext ctx, final VarScope scp, final Var v,
      final Expr e) throws QueryException {
    try {
      final Expr sub = expr.inline(ctx, scp, v, e);
      if(sub == null) return null;
      expr = sub;
    } catch(final QueryException qe) {
      expr = FNInfo.error(qe);
    }
    return this;
  }

  /**
   * Returns this clause as an inlineable expression.
   * @param ex caught exception
   * @param ctx query context
   * @param scp variable scope
   * @return equivalent expression
   * @throws QueryException query exception during inlining
   */
  protected Expr asExpr(final QueryException ex, final QueryContext ctx,
      final VarScope scp) throws QueryException {

    if(expr.isValue()) return expr;
    int i = 0;
    Expr e = expr;
    for(final Value v : values(ex)) {
      final Expr e2 = e.inline(ctx, scp, vars[i++], v);
      if(e2 != null) e = e2;
      if(e.isValue()) break;
    }
    return e;
  }

  /**
   * Returns all error values.
   * @param ex exception
   * @return values
   */
  public static Value[] values(final QueryException ex) {
    final byte[] io = ex.file() == null ? EMPTY : token(ex.file());
    final Value val = ex.value();
    return new Value[] { ex.qname(),
        Str.get(ex.getLocalizedMessage()), val == null ? Empty.SEQ : val,
        Str.get(io), Int.get(ex.line()), Int.get(ex.col()),
        Str.get(ex.getMessage().replaceAll("\r\n?", "\n")) };
  }

  /**
   * Returns the variables used in the {@code catch} expression.
   * @return variables
   */
  public Var[] vars() {
    return vars;
  }

  /**
   * Checks if one of the specified errors match the thrown error.
   * @param qe thrown error
   * @return result of check
   */
  protected boolean matches(final QueryException qe) {
    final QNm code = qe.qname();
    for(final NameTest c : codes) if(c.eq(code)) return true;
    return false;
  }

  @Override
  public String toString() {
    return "catch * { " + expr + " }";
  }

  /**
   * Creates an error QName with the specified name.
   * @param n name
   * @return QName
   */
  private static QNm create(final byte[] n) {
    return new QNm(concat(ERR, COLON, n), ERRORURI);
  }

  @Override
  public boolean accept(final ASTVisitor visitor) {
    for(final Var v : vars) if(!visitor.declared(v)) return false;
    return visitAll(visitor, expr);
  }

  @Override
  public int exprSize() {
    return expr.exprSize();
  }
}
