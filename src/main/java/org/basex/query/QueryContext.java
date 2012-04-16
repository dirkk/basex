package org.basex.query;

import static org.basex.core.Text.*;
import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;
import static org.basex.util.Token.*;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.*;

import org.basex.core.*;
import org.basex.core.Context;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.io.serial.*;
import org.basex.query.expr.*;
import org.basex.query.func.*;
import org.basex.query.item.*;
import org.basex.query.iter.*;
import org.basex.query.up.*;
import org.basex.query.util.*;
import org.basex.query.util.json.*;
import org.basex.query.util.pkg.*;
import org.basex.util.*;
import org.basex.util.ft.*;
import org.basex.util.hash.*;
import org.basex.util.list.*;

/**
 * This class organizes both static and dynamic properties that are specific to a
 * single query.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class QueryContext extends Progress {
  /** URL pattern (matching Clark and EQName notation). */
  private static final Pattern BIND =
      Pattern.compile("^((\"|')(.*?)\\2:|(\\{(.*?)\\}))(.+)$");

  /** Static context of an expression. */
  public StaticContext sc = new StaticContext();
  /** Variables. */
  public final VarContext vars = new VarContext();
  /** Functions. */
  public final UserFuncs funcs = new UserFuncs();

  /** Query resources. */
  public final QueryResources resource = new QueryResources(this);
  /** Database context. */
  public final Context context;
  /** XQuery version flag. */
  public boolean xquery3;

  /** Cached stop word files. */
  public HashMap<String, IO> stop;
  /** Cached thesaurus files. */
  public HashMap<String, IO> thes;
  /** Query options (are valid during query execution). */
  public final HashMap<String, String> dbOptions = new HashMap<String, String>();
  /** Global options (will be set after query execution). */
  public final HashMap<String, Object> globalOpt = new HashMap<String, Object>();

  /** Root expression of the query. */
  public Expr root;
  /** Current context value. */
  public Value value;
  /** Current context position. */
  public long pos = 1;
  /** Current context size. */
  public long size = 1;
  /** Optional initial context set. */
  Nodes nodes;

  /** Current full-text options. */
  private FTOpt ftOpt;
  /** Current full-text token. */
  public FTLexer fttoken;

  /** Current Date. */
  public Dat date;
  /** Current DateTime. */
  public Dtm dtm;
  /** Current Time. */
  public Tim time;

  /** Full-text position data (needed for highlighting of full-text results). */
  public FTPosData ftpos;
  /** Full-text token counter (needed for highlighting of full-text results). */
  public byte ftoknum;

  /** Pending updates. */
  public Updates updates;

  /** Compilation flag: current node has leaves. */
  public boolean leaf;
  /** Compilation flag: GFLWOR clause performs grouping. */
  public boolean grouping;

  /** Number of successive tail calls. */
  public int tailCalls;
  /** Maximum number of successive tail calls. */
  public final int maxCalls;
  /** Counter for variable IDs. */
  public int varIDs;

  /** Pre-declared modules, containing module uri and their file paths. */
  final TokenMap modDeclared = new TokenMap();
  /** Parsed modules, containing the file path and module uri. */
  final TokenMap modParsed = new TokenMap();

  /** Serializer options. */
  SerializerProp serProp;
  /** Initial context value. */
  public Expr ctxItem;
  /** Module loader. */
  public ModuleLoader modules;
  /** Opened connections to relational databases. */
  JDBCConnections jdbc;

  /** String container for query background information. */
  private final TokenBuilder info = new TokenBuilder();
  /** Info flag. */
  private final boolean inf;
  /** Optimization flag. */
  private boolean firstOpt = true;
  /** Evaluation flag. */
  private boolean firstEval = true;

  /**
   * Constructor.
   * @param ctx database context
   */
  public QueryContext(final Context ctx) {
    context = ctx;
    nodes = ctx.current();
    xquery3 = ctx.prop.is(Prop.XQUERY3);
    inf = ctx.prop.is(Prop.QUERYINFO) || Prop.debug;
    final String path = ctx.prop.get(Prop.QUERYPATH);
    if(!path.isEmpty()) sc.baseURI(path);
    maxCalls = ctx.prop.num(Prop.TAILCALLS);
    modules = new ModuleLoader(ctx);
  }

  /**
   * Parses the specified query.
   * @param qu input query
   * @throws QueryException query exception
   */
  public void parse(final String qu) throws QueryException {
    root = new QueryParser(qu, this).parse(null);
  }

  /**
   * Parses the specified module.
   * @param qu input query
   * @return name of module
   * @throws QueryException query exception
   */
  public QNm module(final String qu) throws QueryException {
    return (QNm) new QueryParser(qu, this).parse(EMPTY);
  }

  /**
   * Compiles and optimizes the expression.
   * @throws QueryException query exception
   */
  public void compile() throws QueryException {
    // dump compilation info
    if(inf) compInfo(NL + COMPILING_C);

    // temporarily set database values (size check added for better performance)
    if(!dbOptions.isEmpty()) {
      for(final Entry<String, String> e : dbOptions.entrySet()) {
        context.prop.set(e.getKey(), e.getValue());
      }
    }

    if(ctxItem != null) {
      // evaluate initial expression
      try {
        value = ctxItem.value(this);
      } catch(final QueryException ex) {
        if(ex.err() != XPNOCTX) throw ex;
        // only {@link ParseExpr} instances may cause this error
        CTXINIT.thrw(((ParseExpr) ctxItem).info, ex.getMessage());
      }
    } else if(nodes != null) {
      // add full-text container reference
      if(nodes.ftpos != null) ftpos = new FTPosData();
      // cache the initial context nodes
      resource.compile(nodes);
    }

    // if specified, convert context item to specified type
    if(value != null && sc.initType != null) {
      value = sc.initType.promote(value, this, null);
    }

    try {
      // compile global functions.
      // variables will be compiled if called for the first time
      funcs.comp(this);
      // compile the expression
      if(root != null) root = root.comp(this);
    } catch(final StackOverflowError ex) {
      Util.debug(ex);
      XPSTACK.thrw(null);
    }

    // dump resulting query
    if(inf) info.add(NL + RESULT_C + funcs + root + NL);
  }

  /**
   * Returns a result iterator.
   * @return result iterator
   * @throws QueryException query exception
   */
  public Iter iter() throws QueryException {
    try {
      // evaluate lazily if no updates are possible
      return updating ? value().iter() : iter(root);
    } catch(final StackOverflowError ex) {
      Util.debug(ex);
      throw XPSTACK.thrw(null);
    }
  }

  /**
   * Returns the result value.
   * @return result value
   * @throws QueryException query exception
   */
  public Value value() throws QueryException {
    try {
      final Value v = value(root);
      if(updating) {
        updates.apply();
        if(context.data() != null) context.update();
      }
      return v;

    } catch(final StackOverflowError ex) {
      Util.debug(ex);
      throw XPSTACK.thrw(null);
    }
  }


  /**
   * Evaluates the specified expression and returns an iterator.
   * @param e expression to be evaluated
   * @return iterator
   * @throws QueryException query exception
   */
  public Iter iter(final Expr e) throws QueryException {
    checkStop();
    return e.iter(this);
  }

  /**
   * Evaluates the specified expression and returns an iterator.
   * @param expr expression to be evaluated
   * @return iterator
   * @throws QueryException query exception
   */
  public Value value(final Expr expr) throws QueryException {
    checkStop();
    return expr.value(this);
  }

  /**
   * Returns the current data reference of the context value, or {@code null}.
   * @return data reference
   */
  public Data data() {
    return value != null ? value.data() : null;
  }

  /**
   * Creates a variable with a unique, non-clashing variable name.
   * @param ii input info
   * @param type type
   * @return variable
   */
  public Var uniqueVar(final InputInfo ii, final SeqType type) {
    return Var.create(this, ii, new QNm(token(varIDs)), type, null);
  }

  /**
   * Adds some optimization info.
   * @param string evaluation info
   * @param ext text text extensions
   */
  public void compInfo(final String string, final Object... ext) {
    if(!inf) return;
    if(!firstOpt) info.add(QUERYSEP);
    firstOpt = false;
    info.addExt(string, ext).add(NL);
  }

  /**
   * Adds some evaluation info.
   * @param string evaluation info
   */
  public void evalInfo(final byte[] string) {
    if(!inf) return;
    if(firstEval) info.add(NL).add(EVALUATING_C).add(NL);
    info.add(QUERYSEP).add(string).add(NL);
    firstEval = false;
  }

  /**
   * Returns info on query compilation and evaluation.
   * @return query info
   */
  public String info() {
    return info.toString();
  }

  /**
   * Returns JDBC connections.
   * @return jdbc connections
   */
  public JDBCConnections jdbc() {
    if(jdbc == null) jdbc = new JDBCConnections();
    return jdbc;
  }

  /**
   * Returns the serialization parameters used for and specified by this query.
   * @param optional if {@code true}, a {@code null} reference is returned if no
   *   parameters have been specified
   * @return serialization parameters
   * @throws SerializerException serializer exception
   */
  public SerializerProp serParams(final boolean optional) throws SerializerException {
    // if available, return parameters specified by the query
    if(serProp != null) return serProp;
    // retrieve global parameters
    final String serial = context.prop.get(Prop.SERIALIZER);
    if(optional && serial.isEmpty()) return null;
    // otherwise, if requested, return default parameters
    return new SerializerProp(serial);
  }

  /**
   * Returns the current full-text options. Creates a new instance if called first.
   * @return full-text options
   */
  public FTOpt ftOpt() {
    if(ftOpt == null) ftOpt = new FTOpt();
    return ftOpt;
  }

  /**
   * Sets full-text options.
   * @param opt full-text options
   */
  public void ftOpt(final FTOpt opt) {
    ftOpt = opt;
  }

  /**
   * Returns {@code true} if the query may perform updates.
   * @return updating flag
   */
  public boolean updating() {
    return updating;
  }

  /**
   * Sets the updating flag.
   * @param up updating flag
   */
  public void updating(final boolean up) {
    // initializes the update container
    if(up && updates == null) updates = new Updates();
    updating = up;
  }

  @Override
  public String tit() {
    return EVALUATING_C;
  }

  @Override
  public String det() {
    return EVALUATING_C;
  }

  @Override
  public double prog() {
    return 0;
  }

  // CLASS METHODS ======================================================================


  /**
   * Evaluates the expression with the specified context set.
   * @return resulting value
   * @throws QueryException query exception
   */
  Result execute() throws QueryException {
    // GUI: limit number of hits to be returned and displayed
    int max = context.prop.num(Prop.MAXHITS);
    if(!Prop.gui || max < 0) max = Integer.MAX_VALUE;

    // evaluates the query
    final Iter ir = iter();
    final ValueBuilder vb = new ValueBuilder();
    Item it = null;

    // check if all results belong to the database of the input context
    if(serProp == null && nodes != null) {
      final IntList pre = new IntList();

      while((it = ir.next()) != null) {
        checkStop();
        if(!(it instanceof DBNode) || it.data() != nodes.data) break;
        if(pre.size() < max) pre.add(((DBNode) it).pre);
      }

      final int ps = pre.size();
      if(it == null || ps == max) {
        // all nodes have been processed: return GUI-friendly nodeset
        return ps == 0 ? vb : new Nodes(pre.toArray(), nodes.data, ftpos).checkRoot();
      }

      // otherwise, add nodes to standard iterator
      for(int p = 0; p < ps; ++p) vb.add(new DBNode(nodes.data, pre.get(p)));
      vb.add(it);
    }

    // use standard iterator
    while((it = ir.next()) != null) {
      checkStop();
      if(vb.size() < max) vb.add(it);
    }
    return vb;
  }

  /**
   * Binds a value to the context item, using the same rules as for
   * {@link #bind binding variables}.
   * @param val value to be bound
   * @param type data type (may be {@code null})
   * @throws QueryException query exception
   */
  void context(final Object val, final String type) throws QueryException {
    ctxItem = cast(val, type);
  }

  /**
   * Binds a value to a global variable. The specified type is interpreted as follows:
   * <ul>
   * <li>If {@code "json"} is specified, the value is converted according to the rules
   *     specified in {@link JsonMapConverter}.</li>
   * <li>If {@code "xml"} is specified, the value is converted to a document node.</li>
   * <li>Otherwise, the type is interpreted as atomic XDM data type.</li>
   * </ul>
   * If the value is an XQuery expression or value {@link Expr}, it is directly assigned.
   * Otherwise, it is cast to the XQuery data model, using a Java/XQuery mapping.
   * @param name name of variable
   * @param val value to be bound
   * @param type data type (may be {@code null})
   * @throws QueryException query exception
   */
  void bind(final String name, final Object val, final String type)
      throws QueryException {
    bind(name, cast(val, type));
  }

  /**
   * Recursively serializes the query plan.
   * @param ser serializer
   * @throws IOException I/O exception
   */
  void plan(final Serializer ser) throws IOException {
    // only show root node if functions or variables exist
    final boolean r = funcs.funcs().length != 0 || vars.globals().size != 0;
    if(r) ser.openElement(PLAN);
    funcs.plan(ser);
    vars.plan(ser);
    root.plan(ser);
    if(r) ser.closeElement();
  }

  // PRIVATE METHODS ====================================================================

  /**
   * Binds an value to a global variable. If the value is an {@link Expr}
   * instance, it is directly assigned. Otherwise, it is first cast to the
   * appropriate XQuery type.
   * @param name name of variable
   * @param val value to be bound
   * @throws QueryException query exception
   */
  private void bind(final String name, final Expr val) throws QueryException {
    // remove optional $ prefix
    String nm = name.indexOf('$') == 0 ? name.substring(1) : name;
    byte[] uri = EMPTY;

    // check for namespace declaration
    final Matcher m = BIND.matcher(nm);
    if(m.find()) {
      String u = m.group(3);
      if(u == null) u = m.group(5);
      uri = token(u);
      nm = m.group(6);
    }
    final byte[] ln = token(nm);
    if(nm.isEmpty() || !XMLToken.isNCName(ln)) return;

    // bind variable
    final QNm qnm = uri.length == 0 ? new QNm(ln, this) : new QNm(ln, uri);
    final Var gl = vars.globals().get(qnm);
    if(gl == null) {
      // assign new variable
      vars.updateGlobal(Var.create(this, null, qnm, null).bind(val, this));
    } else {
      // reset declaration state and bind new expression
      gl.declared = false;
      gl.bind(gl.type == null ? val :
        gl.type.type.cast(val.item(this, null), this, null), this);
    }
  }

  /**
   * Casts a value to the specified type.
   * See {@link #bind(String, Object, String)} for more infos.
   * @param val value to be cast
   * @param type data type (may be {@code null})
   * @return cast value
   * @throws QueryException query exception
   */
  private Expr cast(final Object val, final String type) throws QueryException {
    // return original value
    if(type == null || type.isEmpty()) {
      return val instanceof Expr ? (Expr) val : JavaMapping.toValue(val);
    }

    // convert to json
    if(type.equalsIgnoreCase(JSONSTR)) {
      return JsonMapConverter.parse(token(val.toString()), null);
    }

    // convert to xml
    if(type.equalsIgnoreCase(XMLSTR)) {
      try {
        return new DBNode(new IOContent(val.toString()), context.prop);
      } catch(final IOException ex) {
        throw SAXERR.thrw(null, ex);
      }
    }

    // convert to the specified type
    final QNm nm = new QNm(token(type), this);
    if(!nm.hasURI() && nm.hasPrefix()) NOURI.thrw(null, nm);
    final Type typ = AtomType.find(nm, false);
    if(typ == null) NOTYPE.thrw(null, nm);
    return typ.cast(val, null);
  }
}
