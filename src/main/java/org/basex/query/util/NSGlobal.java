package org.basex.query.util;

import static org.basex.query.QueryTokens.*;
import static org.basex.util.Token.*;
import org.basex.util.Atts;

/**
 * Global namespaces.
 *
 * @author Workgroup DBIS, University of Konstanz 2005-10, ISC License
 * @author Christian Gruen
 */
public final class NSGlobal {
  /** Namespaces. */
  private static final Atts NS = new Atts();

  static {
    NS.add(LOCAL, LOCALURI);
    NS.add(XS, XSURI);
    NS.add(XSI, XSIURI);
    NS.add(OUTPUT, OUTPUTURI);
    NS.add(FN, FNURI);
    NS.add(MATH, MATHURI);
    NS.add(XMLNS, XMLNSURI);
    NS.add(XML, XMLURI);
    NS.add(BASEX, BXURI);
    NS.add(SENT, SENTURI);
    NS.add(FILE, FILEURI);
  }

  /** Private constructor. */
  private NSGlobal() { }

  /**
   * Finds the specified namespace uri.
   * @param pre prefix of the namespace
   * @return uri
   */
  public static byte[] uri(final byte[] pre) {
    for(int s = NS.size - 1; s >= 0; s--) {
      if(eq(NS.key[s], pre)) return NS.val[s];
    }
    return EMPTY;
  }

  /**
   * Finds the specified URI prefix.
   * @param uri URI
   * @return prefix
   */
  static byte[] prefix(final byte[] uri) {
    for(int s = NS.size - 1; s >= 0; s--) {
      if(eq(NS.val[s], uri)) return NS.key[s];
    }
    return EMPTY;
  }

  /**
   * Checks if the specified uri is a standard uri.
   * @param uri uri to be checked
   * @return result of check
   */
  public static boolean standard(final byte[] uri) {
    // 'local' namespace is skipped
    for(int s = NS.size - 1; s > 0; s--) {
      if(eq(NS.val[s], uri)) return true;
    }
    return false;
  }
}
