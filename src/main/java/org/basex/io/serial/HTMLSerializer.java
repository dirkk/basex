package org.basex.io.serial;

import static org.basex.data.DataText.*;
import static org.basex.query.util.Err.SERILL;
import static org.basex.query.util.Err.SERPI;
import static org.basex.util.Token.*;

import org.basex.util.hash.TokenSet;
import org.basex.util.list.TokenList;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class serializes data as HTML.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public class HTMLSerializer extends OutputSerializer {
  /** (X)HTML: elements with an empty content model. */
  static final TokenList EMPTIES = new TokenList();
  /** (X)HTML: URI attributes. */
  static final TokenSet URIS = new TokenSet();

  /** HTML: script elements. */
  private static final TokenList SCRIPTS = new TokenList();
  /** HTML: boolean attributes. */
  private static final TokenSet BOOLEAN = new TokenSet();

  /**
   * Constructor, specifying serialization options.
   * @param os output stream reference
   * @param p serialization properties
   * @throws IOException I/O exception
   */
  HTMLSerializer(final OutputStream os, final SerializerProp p) throws IOException {
    super(os, p, V40, V401);
  }

  @Override
  public void attribute(final byte[] n, final byte[] v) throws IOException {
    // don't append value for boolean attributes
    final byte[] tagatt = concat(lc(tag), COLON, lc(n));
    if(BOOLEAN.contains(tagatt) && eq(n, v)) return;
    // escape URI attributes
    final byte[] val = escape && URIS.contains(tagatt) ? escape(v) : v;

    print(' ');
    print(n);
    print(ATT1);
    for(int k = 0; k < val.length; k += cl(val, k)) {
      final int ch = cp(val, k);
      if(ch == '<' || ch == '&' &&
          val[Math.min(k + 1, val.length - 1)] == '{') {
        print(ch);
      } else if(ch == '"') {
        print(E_QU);
      } else if(ch == 0x9 || ch == 0xA) {
        hex(ch);
      } else {
        code(ch);
      }
    }
    print(ATT2);
  }

  @Override
  public void finishComment(final byte[] n) throws IOException {
    if(sep) indent();
    print(COMM_O);
    print(n);
    print(COMM_C);
  }

  @Override
  public void finishPi(final byte[] n, final byte[] v) throws IOException {
    if(sep) indent();
    if(contains(v, '>')) SERPI.thrwSerial();
    print(PI_O);
    print(n);
    print(' ');
    print(v);
    print(ELEM_C);
  }

  @Override
  protected void code(final int ch) throws IOException {
    if(script) printChar(ch);
    else if(ch > 0x7F && ch < 0xA0) SERILL.thrwSerial(Integer.toHexString(ch));
    else if(ch == 0xA0) print(E_NBSP);
    else super.code(ch);
  }

  @Override
  protected void startOpen(final byte[] t) throws IOException {
    doctype(null);
    if(sep) indent();
    print(ELEM_O);
    print(t);
    sep = indent;
    script = SCRIPTS.contains(lc(t));
    if(content && eq(lc(tag), HEAD)) ct++;
  }

  @Override
  protected void finishOpen() throws IOException {
    super.finishOpen();
    ct(false, true);
  }

  @Override
  protected void finishEmpty() throws IOException {
    if(ct(true, true)) return;
    print(ELEM_C);
    if(EMPTIES.contains(lc(tag))) return;
    sep = false;
    finishClose();
  }

  @Override
  protected void finishClose() throws IOException {
    super.finishClose();
    script = script && !SCRIPTS.contains(lc(tag));
  }

  // HTML Serializer: cache elements
  static {
    // script elements
    SCRIPTS.add(token("script"));
    SCRIPTS.add(token("style"));
    // boolean attributes
    BOOLEAN.add(token("area:nohref"));
    BOOLEAN.add(token("button:disabled"));
    BOOLEAN.add(token("dir:compact"));
    BOOLEAN.add(token("dl:compact"));
    BOOLEAN.add(token("frame:noresize"));
    BOOLEAN.add(token("hr:noshade"));
    BOOLEAN.add(token("img:ismap"));
    BOOLEAN.add(token("input:checked"));
    BOOLEAN.add(token("input:disabled"));
    BOOLEAN.add(token("input:readonly"));
    BOOLEAN.add(token("menu:compact"));
    BOOLEAN.add(token("object:declare"));
    BOOLEAN.add(token("ol:compact"));
    BOOLEAN.add(token("optgroup:disabled"));
    BOOLEAN.add(token("option:selected"));
    BOOLEAN.add(token("option:disabled"));
    BOOLEAN.add(token("script:defer"));
    BOOLEAN.add(token("select:multiple"));
    BOOLEAN.add(token("select:disabled"));
    BOOLEAN.add(token("td:nowrap"));
    BOOLEAN.add(token("textarea:disabled"));
    BOOLEAN.add(token("textarea:readonly"));
    BOOLEAN.add(token("th:nowrap"));
    BOOLEAN.add(token("ul:compact"));
    // elements with an empty content model
    EMPTIES.add(token("area"));
    EMPTIES.add(token("base"));
    EMPTIES.add(token("br"));
    EMPTIES.add(token("col"));
    EMPTIES.add(token("hr"));
    EMPTIES.add(token("img"));
    EMPTIES.add(token("input"));
    EMPTIES.add(token("link"));
    EMPTIES.add(token("meta"));
    EMPTIES.add(token("basefont"));
    EMPTIES.add(token("frame"));
    EMPTIES.add(token("isindex"));
    EMPTIES.add(token("param"));
    // URI attributes
    URIS.add(token("a:href"));
    URIS.add(token("a:name"));
    URIS.add(token("applet:codebase"));
    URIS.add(token("area:href"));
    URIS.add(token("base:href"));
    URIS.add(token("blockquote:cite"));
    URIS.add(token("body:background"));
    URIS.add(token("button:datasrc"));
    URIS.add(token("del:cite"));
    URIS.add(token("div:datasrc"));
    URIS.add(token("form:action"));
    URIS.add(token("frame:longdesc"));
    URIS.add(token("frame:src"));
    URIS.add(token("head:profile"));
    URIS.add(token("iframe:longdesc"));
    URIS.add(token("iframe:src"));
    URIS.add(token("img:longdesc"));
    URIS.add(token("img:src"));
    URIS.add(token("img:usemap"));
    URIS.add(token("input:datasrc"));
    URIS.add(token("input:src"));
    URIS.add(token("input:usemap"));
    URIS.add(token("ins:cite"));
    URIS.add(token("link:href"));
    URIS.add(token("object:archive"));
    URIS.add(token("object:classid"));
    URIS.add(token("object:codebase"));
    URIS.add(token("object:data"));
    URIS.add(token("object:datasrc"));
    URIS.add(token("object:usemap"));
    URIS.add(token("q:cite"));
    URIS.add(token("script:for"));
    URIS.add(token("script:src"));
    URIS.add(token("select:datasrc"));
    URIS.add(token("span:datasrc"));
    URIS.add(token("table:datasrc"));
    URIS.add(token("textarea:datasrc"));
  }
}
