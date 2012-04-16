package org.basex.gui.layout;

import java.awt.*;

import org.basex.gui.*;
import org.basex.gui.GUIConstants.Fill;
import org.basex.util.*;

/**
 * Efficient Text Editor and Renderer, supporting syntax highlighting and
 * text selections.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
final class BaseXTextRenderer extends BaseXBack {
  /** Vertical start position. */
  private final BaseXBar bar;

  /** Font. */
  private Font font;
  /** Default font. */
  private Font dfont;
  /** Bold font. */
  private Font bfont;
  /** Font height. */
  private int fontH;
  /** Character widths. */
  private int[] fwidth = GUIConstants.mfwidth;
  /** Color. */
  private Color color;
  /** Color highlighting flag. */
  private boolean high;

  /** Width of current word. */
  private int wordW;
  /** Search keyword. */
  private String keyword = "";

  /** Border offset. */
  private int off;
  /** Current x coordinate. */
  private int x;
  /** Current y coordinate. */
  private int y;
  /** Current width. */
  private int w;
  /** Current height. */
  private int h;

  /** Text array to be written. */
  private transient BaseXTextTokens text;
  /** Vertical start position. */
  private transient BaseXSyntax syntax = BaseXSyntax.SIMPLE;
  /** Visibility of cursor. */
  private boolean cursor;

  /**
   * Constructor.
   * @param t text to be drawn
   * @param b scrollbar reference
   */
  BaseXTextRenderer(final BaseXTextTokens t, final BaseXBar b) {
    mode(Fill.NONE).setFont(GUIConstants.dfont);
    text = t;
    bar = b;
  }

  @Override
  public void setFont(final Font f) {
    dfont = f;
    bfont = f.deriveFont(Font.BOLD);
    font(f);
  }

  @Override
  public void paintComponent(final Graphics g) {
    super.paintComponent(g);
    write(g, bar.pos());
  }

  /**
   * Initializes the constructor.
   * @param t text to be drawn
   */
  void setText(final BaseXTextTokens t) {
    text = t;
  }

  /**
   * Sets a new keyword.
   * @param key new keyword
   * @return old keyword
   */
  String keyword(final String key) {
    final String k = keyword;
    keyword = key;
    return k;
  }

  /**
   * Finds the current keyword and returns the text position.
   * @param forward backward browsing
   * @param same string is the same as last time
   * @return new position
   */
  int find(final boolean forward, final boolean same) {
    if(keyword.isEmpty()) return 0;

    while(true) {
      final int hh = h;
      int lp = 0;
      int ly = 0;
      int cp = text.cursor();

      h = Integer.MAX_VALUE;
      final Graphics g = getGraphics();
      for(init(g, 0); more(g); next()) {
        if(!found()) continue;

        final int np = text.pos();
        final int ny = y - fontH;
        if(np >= cp && (np > cp || !same || !forward)) {
          if(forward || lp != 0) {
            h = hh;
            text.setCaret(forward ? np : lp);
            return forward ? ny : ly;
          }
          cp = Integer.MAX_VALUE;
        }
        lp = np;
        ly = ny;
      }

      h = hh;
      if(cp == 0 || cp == Integer.MAX_VALUE) {
        text.setCaret(lp);
        return ly;
      }
      text.setCaret(0);
    }
  }

  /**
   * Returns the cursor coordinates.
   * @return line/column
   */
  int[] pos() {
    final int hh = h;
    h = Integer.MAX_VALUE;
    final Graphics g = getGraphics();
    int col = 1;
    int line = 1;
    init(g, 0);
    boolean more = true;
    while(more(g)) {
      final int p = text.pos();
      while(text.more()) {
        more = text.pos() < text.cursor();
        if(!more) break;
        text.next();
        col++;
      }
      if(!more) break;
      text.pos(p);
      if(next()) {
        line++;
        col = 1;
      }
    }
    h = hh;
    return new int[] { line, col };
  }

  /**
   * Sets the current font.
   * @param f font
   */
  private void font(final Font f) {
    font = f;
    off = f.getSize() + 1 >> 2;
    fontH = f.getSize() + off;
    fwidth = GUIConstants.fontWidths(f);
  }

  @Override
  public Dimension getPreferredSize() {
    final Graphics g = getGraphics();
    w = Integer.MAX_VALUE;
    h = Integer.MAX_VALUE;
    init(g, 0);
    int max = 0;
    while(more(g)) {
      if(text.curr() == 0x0A) max = Math.max(x, max);
      next();
    }
    return new Dimension(Math.max(x, max) + fwidth[' '], y + fontH);
  }

  /**
   * Initializes the renderer.
   * @param g graphics reference
   * @param pos current text position
   */
  private void init(final Graphics g, final int pos) {
    font = dfont;
    color = Color.black;
    syntax.init();
    text.init();
    x = off;
    y = off + fontH - pos - 2;
    if(g != null) g.setFont(font);
  }

  /**
   * Calculates the text height.
   */
  void calc() {
    w = getWidth() - (off >> 1);
    h = Integer.MAX_VALUE;
    final Graphics g = getGraphics();
    init(g, 0);
    while(more(g)) next();
    h = getHeight() + fontH;
    bar.height(y + off);
  }

  /**
   * Returns the current vertical cursor position.
   * @return new position
   */
  int cursorY() {
    final int hh = h;
    h = Integer.MAX_VALUE;
    final Graphics g = getGraphics();
    init(g, 0);
    while(more(g) && !text.edited()) next();
    h = hh;
    return y - fontH;
  }

  /**
   * Writes the text.
   * @param g graphics reference
   * @param pos current text position
   */
  private void write(final Graphics g, final int pos) {
    init(g, pos);
    while(more(g)) write(g);
    if(cursor && text.cursor() == text.size()) cursor(g, x);
  }

  /**
   * Checks if the text has more words to print.
   * @param g graphics reference
   * @return true if the text has more words
   */
  private boolean more(final Graphics g) {
    // no more words found; quit
    if(!text.moreWords()) return false;

    // calculate word width
    int ww = 0;
    final int p = text.pos();
    while(text.more()) {
      final int ch = text.next();
      // internal special codes...
      if(ch == 0x02) {
        font(bfont);
      } else if(ch == 0x03) {
        font(dfont);
      } else {
        ww += charW(g, ch);
      }
    }
    text.pos(p);

    // jump to new line
    if(x + ww > w) {
      x = off;
      y += fontH;
    }
    wordW = ww;

    // check if word has been found, and word is still visible
    return y < h;
  }

  /**
   * Finishes the current token.
   * @return true for new line
   */
  private boolean next() {
    final int ch = text.curr();
    if(ch == TokenBuilder.NLINE || ch == TokenBuilder.HLINE) {
      x = off;
      y += fontH >> (ch == TokenBuilder.NLINE ? 0 : 1);
      return true;
    }
    x += wordW;
    return false;
  }

  /**
   * Writes the current string to the graphics reference.
   * @param g graphics reference
   */
  private void write(final Graphics g) {
    if(high) {
      high = false;
    } else {
      color = isEnabled() ? syntax.getColor(text) : Color.gray;
    }

    final int ch = text.curr();
    if(y > 0 && y < h) {
      if(ch == TokenBuilder.MARK) {
        color = GUIConstants.GREEN;
        high = true;
      }

      // mark error
      if(text.error()) {
        g.setColor(GUIConstants.LRED);
        g.fillRect(x, y - fontH + 4, wordW, fontH);
      }

      // mark text
      int xx = x;
      if(text.markStart()) {
        final int p = text.pos();
        while(text.more()) {
          final int cw = charW(g, text.curr());
          if(text.inMark()) {
            g.setColor(GUIConstants.color(3));
            g.fillRect(xx, y - fontH + 4, cw, fontH);
          }
          xx += cw;
          text.next();
        }
        text.pos(p);
      }
      if(found()) {
        int cw = 0;
        for(int c = 0; c < keyword.length(); ++c) {
          cw += charW(g, keyword.charAt(c));
        }
        g.setColor(GUIConstants.color(text.cursor() == text.pos() ? 5 : 2));
        g.fillRect(x, y - fontH + 4, cw, fontH);
      }

      // don't write whitespaces
      if(ch > ' ') {
        g.setColor(color);
        String n = text.nextWord();
        int ww = w - x;
        if(x + wordW > ww) {
          // shorten string if it cannot be completely shown (saves memory)
          int c = 0;
          for(final int nl = n.length(); c < nl && ww > 0; c++) {
            ww -= charW(g, n.charAt(c));
          }
          n = n.substring(0, c);
        }
        g.drawString(n, x, y);
      } else if(ch <= TokenBuilder.MARK) {
        g.setFont(font);
      }

      // show cursor
      if(cursor && text.edited()) {
        xx = x;
        final int p = text.pos();
        while(text.more()) {
          if(text.cursor() == text.pos()) {
            cursor(g, xx);
            break;
          }
          xx += charW(g, text.next());
        }
        text.pos(p);
      }
    }
    next();
  }

  /**
   * Returns true if the keyword is found.
   * @return result of check
   */
  private boolean found() {
    if(keyword.isEmpty()) return false;

    final int sl = keyword.length();
    final int wl = text.length();
    if(wl < sl) return false;
    final int p = text.pos();
    int s = -1;
    while(++s != sl) {
      if(Character.toLowerCase(text.next()) != keyword.charAt(s)) break;
    }
    text.pos(p);
    return s == sl;
  }

  /**
   * Paints the text cursor.
   * @param g graphics reference
   * @param xx x position
   */
  private void cursor(final Graphics g, final int xx) {
    g.setColor(Color.black);
    g.drawLine(xx, y - fontH + 4, xx, y + 3);
  }

  /**
   * Finishes the selection.
   */
  void stopSelect() {
    text.checkMark();
  }

  /**
   * Selects the text at the specified position.
   * @param pos current text position
   * @param p mouse position
   * @param finish states if selection is in progress
   */
  void select(final int pos, final Point p, final boolean finish) {
    if(!finish) text.noMark();
    p.y -= 3;

    final Graphics g = getGraphics();
    init(g, pos);
    if(p.y > y - fontH) {
      int s = text.pos();
      while(true) {
        // end of line
        if(p.x > x && p.y < y - fontH) {
          text.pos(s);
          break;
        }
        // end of text - skip last characters
        if(!more(g)) {
          while(text.more()) text.next();
          break;
        }
        // beginning of line
        if(p.x <= x && p.y < y) break;
        // middle of line
        if(p.x > x && p.x <= x + wordW && p.y > y - fontH && p.y <= y) {
          while(text.more()) {
            final int ww = charW(g, text.curr());
            if(p.x < x + ww) break;
            x += ww;
            text.next();
          }
          break;
        }
        s = text.pos();
        next();
      }

      if(!finish) text.startMark();
      else text.endMark();
      text.setCaret();
    }
    repaint();
  }

  /**
   * Returns the width of the specified codepoint.
   * @param g graphics reference
   * @param cp character
   * @return width
   */
  private int charW(final Graphics g, final int cp) {
    return cp < ' ' || g == null ?  cp == '\t' ?
      fwidth[' '] * BaseXTextTokens.TAB : 0 : cp < 256 ? fwidth[cp] :
      cp >= 0xD800 && cp <= 0xDC00 ? 0 : g.getFontMetrics().charWidth(cp);
  }

  /**
   * Returns the font height.
   * @return font height
   */
  int fontH() {
    return fontH;
  }

  /**
   * Sets the cursor flag.
   * @param c cursor flag
   */
  void cursor(final boolean c) {
    cursor = c;
  }

  /**
   * Returns the cursor flag.
   * @return cursor flag
   */
  boolean cursor() {
    return cursor;
  }

  /**
   * Sets a syntax highlighter.
   * @param s syntax highlighter
   */
  void setSyntax(final BaseXSyntax s) {
    syntax = s;
  }

  /**
   * Returns the syntax highlighter.
   * @return syntax highlighter
   */
  BaseXSyntax getSyntax() {
    return syntax;
  }
}
