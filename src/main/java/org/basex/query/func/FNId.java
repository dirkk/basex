package org.basex.query.func;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;
import static org.basex.util.Token.*;

import org.basex.query.*;
import org.basex.query.expr.*;
import org.basex.query.iter.*;
import org.basex.query.value.item.*;
import org.basex.query.value.node.*;
import org.basex.query.value.type.*;
import org.basex.util.*;
import org.basex.util.list.*;

/**
 * ID functions.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class FNId extends StandardFunc {
  /**
   * Constructor.
   * @param ii input info
   * @param f function definition
   * @param e arguments
   */
  public FNId(final InputInfo ii, final Function f, final Expr... e) {
    super(ii, f, e);
  }

  @Override
  public Iter iter(final QueryContext ctx) throws QueryException {
    // functions have 1 or 2 arguments...
    final ANode node = checkNode(expr.length == 2 ? expr[1] : checkCtx(ctx), ctx);

    switch(sig) {
      case ID:              return id(ctx.iter(expr[0]), node);
      case IDREF:           return idref(ctx.iter(expr[0]), node);
      case ELEMENT_WITH_ID: return elid(ctx.iter(expr[0]), node);
      default:              return super.iter(ctx);
    }
  }

  @Override
  public Item item(final QueryContext ctx, final InputInfo ii) throws QueryException {
    // functions have 1 or 2 arguments...
    final ANode node = checkNode(expr.length == 2 ? expr[1] : checkCtx(ctx), ctx);

    switch(sig) {
      case LANG:  return lang(lc(checkEStr(expr[0], ctx)), node);
      default:    return super.item(ctx, ii);
    }
  }

  /**
   * Returns the parent result of the ID function.
   * @param it item ids to be found
   * @param node attribute
   * @return resulting node list
   * @throws QueryException query exception
   */
  private Iter elid(final Iter it, final ANode node) throws QueryException {
    return id(it, node);
    /*
    final NodeCache nc = id(it, node);
    final NodeCache res = new NodeCache().random();
    for(ANode n; (n = nc.next()) != null;) res.add(n.parent());
    return res;
    */
  }

  /**
   * Returns the result of the ID function.
   * @param it item ids to be found
   * @param node attribute
   * @return resulting node list
   * @throws QueryException query exception
   */
  private NodeSeqBuilder id(final Iter it, final ANode node) throws QueryException {
    final NodeSeqBuilder nc = new NodeSeqBuilder().check();
    add(ids(it), nc, checkRoot(node));
    return nc;
  }

  /**
   * Returns the result of the IDREF function.
   * @param it item ids to be found
   * @param node attribute
   * @return resulting node list
   * @throws QueryException query exception
   */
  private Iter idref(final Iter it, final ANode node) throws QueryException {
    final NodeSeqBuilder nb = new NodeSeqBuilder().check();
    addRef(ids(it), nb, checkRoot(node));
    return nb;
  }

  /**
   * Returns the result of the language function.
   * @param lang language to be found
   * @param node attribute
   * @return resulting node list
   */
  private static Bln lang(final byte[] lang, final ANode node) {
    for(ANode n = node; n != null; n = n.parent()) {
      final AxisIter atts = n.attributes();
      for(ANode at; (at = atts.next()) != null;) {
        if(eq(at.qname().string(), LANG)) {
          final byte[] ln = lc(norm(at.string()));
          return Bln.get(startsWith(ln, lang) &&
              (lang.length == ln.length || ln[lang.length] == '-'));
        }
      }
    }
    return Bln.FALSE;
  }

  /**
   * Extracts the ids from the specified iterator.
   * @param iter iterator
   * @return ids
   * @throws QueryException query exception
   */
  private byte[][] ids(final Iter iter) throws QueryException {
    final TokenList tl = new TokenList();
    for(Item id; (id = iter.next()) != null;) {
      for(final byte[] i : split(norm(checkEStr(id)), ' ')) tl.add(i);
    }
    return tl.toArray();
  }

  /**
   * Adds nodes with the specified id.
   * @param ids ids to be found
   * @param nc node cache
   * @param node node
   */
  private static void add(final byte[][] ids, final NodeSeqBuilder nc, final ANode node) {
    AxisIter ai = node.attributes();
    for(ANode at; (at = ai.next()) != null;) {
      final byte[] val = at.string();
      // [CG] XQuery: ID-IDREF Parsing
      for(final byte[] id : ids) {
        if(!eq(val, id)) continue;
        final byte[] nm = lc(at.qname().string());
        if(contains(nm, ID) && !contains(nm, IDREF)) nc.add(node);
      }
    }
    ai = node.children();
    for(ANode att; (att = ai.next()) != null;) add(ids, nc, att.finish());
  }

  /**
   * Adds nodes with the specified id.
   * @param ids ids to be found
   * @param nc node cache
   * @param node node
   */
  private static void addRef(final byte[][] ids, final NodeSeqBuilder nc,
      final ANode node) {

    AxisIter ai = node.attributes();
    for(ANode at; (at = ai.next()) != null;) {
      final byte[] val = at.string();
      // [CG] XQuery: ID-IDREF Parsing
      for(final byte[] id : ids) {
        if(!eq(val, id)) continue;
        final byte[] nm = lc(at.qname().string());
        if(contains(nm, IDREF)) nc.add(at.finish());
      }
    }
    ai = node.children();
    for(ANode att; (att = ai.next()) != null;) addRef(ids, nc, att.finish());
  }

  /**
   * Checks if the specified node has a document node as root.
   * @param node input node
   * @return specified node
   * @throws QueryException query exception
   */
  private ANode checkRoot(final ANode node) throws QueryException {
    if(node instanceof FNode) {
      ANode n = node;
      while(n.type != NodeType.DOC) {
        n = n.parent();
        if(n == null) throw IDDOC.thrw(info);
      }
    }
    return node;
  }

  @Override
  public boolean has(final Flag flag) {
    return flag == Flag.CTX && expr.length == 1 || super.has(flag);
  }
}
