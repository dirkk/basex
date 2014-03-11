package org.basex.query.up.expr;

import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.expr.Arr;
import org.basex.query.expr.Expr;
import org.basex.query.gflwor.Let;
import org.basex.query.iter.Iter;
import org.basex.query.iter.ValueIter;
import org.basex.query.up.ContextModifier;
import org.basex.query.up.TransformModifier;
import org.basex.query.up.Updates;
import org.basex.query.util.ASTVisitor;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.node.ANode;
import org.basex.query.value.node.FElem;
import org.basex.query.var.Var;
import org.basex.query.var.VarScope;
import org.basex.query.var.VarUsage;
import org.basex.util.InputInfo;
import org.basex.util.hash.IntObjMap;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.UPCOPYMULT;
import static org.basex.query.util.Err.UPMODIFY;

/**
 * Transform expression.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Lukas Kircher
 */
public final class Transform extends Arr {
  /** Variable bindings created by copy clause. */
  private final Let[] copies;

  /**
   * Constructor.
   * @param info input info
   * @param copies copy expressions
   * @param mod modify expression
   * @param ret return expression
   */
  public Transform(final InputInfo info, final Let[] copies, final Expr mod, final Expr ret) {
    super(info, mod, ret);
    this.copies = copies;
  }

  @Override
  public void checkUp() throws QueryException {
    for(final Let c : copies) c.checkUp();
    final Expr m = expr[0];
    m.checkUp();
    if(!m.isVacuous() && !m.has(Flag.UPD)) throw UPMODIFY.get(info);
    checkNoUp(expr[1]);
  }

  @Override
  public Expr compile(final QueryContext ctx, final VarScope scp) throws QueryException {
    for(final Let c : copies) c.expr = c.expr.compile(ctx, scp);
    super.compile(ctx, scp);
    return this;
  }

  @Override
  public ValueIter iter(final QueryContext ctx) throws QueryException {
    return value(ctx).iter();
  }

  @Override
  public Value value(final QueryContext ctx) throws QueryException {
    final int o = (int) ctx.output.size();
    if(ctx.updates == null) ctx.updates = new Updates();
    final ContextModifier tmp = ctx.updates.mod;
    final TransformModifier pu = new TransformModifier();
    ctx.updates.mod = pu;

    try {
      for(final Let fo : copies) {
        final Iter ir = ctx.iter(fo.expr);
        Item i = ir.next();
        if(!(i instanceof ANode) || ir.next() != null) throw UPCOPYMULT.get(fo.info, fo.var.name);

        // copy node to main memory data instance
        i = ((ANode) i).dbCopy(ctx.context.options);
        // add resulting node to variable
        ctx.set(fo.var, i, info);
        pu.addData(i.data());
      }
      ctx.value(expr[0]);
      ctx.updates.apply(ctx.context.triggers);
      return ctx.value(expr[1]);
    } finally {
      ctx.output.size(o);
      ctx.updates.mod = tmp;
    }
  }

  @Override
  public boolean has(final Flag flag) {
    return flag != Flag.UPD && super.has(flag);
  }

  @Override
  public boolean removable(final Var v) {
    for(final Let c : copies) if(!c.removable(v)) return false;
    return super.removable(v);
  }

  @Override
  public VarUsage count(final Var v) {
    return VarUsage.sum(v, copies).plus(super.count(v));
  }

  @Override
  public Expr inline(final QueryContext ctx, final VarScope scp,
      final Var v, final Expr e) throws QueryException {
    final boolean cp = inlineAll(ctx, scp, copies, v, e);
    return inlineAll(ctx, scp, expr, v, e) || cp ? optimize(ctx, scp) : null;
  }

  @Override
  public Expr copy(final QueryContext ctx, final VarScope scp, final IntObjMap<Var> vs) {
    return new Transform(info, copyAll(ctx, scp, vs, copies), expr[0].copy(ctx, scp, vs),
        expr[1].copy(ctx, scp, vs));
  }

  @Override
  public void plan(final FElem plan) {
    addPlan(plan, planElem(), copies, expr);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(COPY + ' ');
    for(final Let t : copies)
      sb.append(t.var).append(' ').append(ASSIGN).append(' ').append(t.expr).append(' ');
    return sb.append(MODIFY + ' ' + expr[0] + ' ' + RETURN + ' ' + expr[1]).toString();
  }

  @Override
  public boolean accept(final ASTVisitor visitor) {
    return visitAll(visitor, copies) && super.accept(visitor);
  }

  @Override
  public int exprSize() {
    int sz = 1;
    for(final Let lt : copies) sz += lt.exprSize();
    for(final Expr e : expr) sz += e.exprSize();
    return sz;
  }
}
