package org.basex.test.query.expr;

import static org.junit.Assert.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.test.*;
import org.basex.util.*;
import org.junit.*;

/**
 * Test cases for FLWOR expressions.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Leo Woerteler
 */
public final class FLWORTest extends SandboxTest {
  /** Tests shadowing of outer variables. */
  @Test
  public void shadowTest() {
    query("for $a in for $a in <a>1</a> return $a/text() return <x>{ $a }</x>",
        "<x>1</x>");
  }

  /**
   * Runs an updating query and matches the result of the second query
   * against the expected output.
   * @param query query
   * @param expected expected output
   */
  private static void query(final String query, final String expected) {
    try {
      final String result = new XQuery(query).execute(context);
      // quotes are replaced by apostrophes to simplify comparison
      assertEquals(expected.replaceAll("\"", "'"),
          result.replaceAll("\"", "'"));
    } catch(final BaseXException ex) {
      fail(Util.message(ex));
    }
  }
}
