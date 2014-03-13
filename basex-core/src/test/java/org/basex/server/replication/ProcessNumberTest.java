package org.basex.server.replication;

import org.basex.server.election.ProcessNumber;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * ProcessNumber implements the {@link java.lang.Comparable} interface. It should be compared
 * based on an integer value named {@literal weight} and if this is equal based on the
 * {@literal id} string.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ProcessNumberTest {
  @Test
  public void compareUnequalWeight() {
    final ProcessNumber p1 = new ProcessNumber(1, "test");
    final ProcessNumber p2 = new ProcessNumber(2, "test");
    final ProcessNumber p3 = new ProcessNumber(2, "test");

    assertEquals(-1, p1.compareTo(p2));
    assertEquals(1, p2.compareTo(p1));
    assertEquals(0, p2.compareTo(p3));
    assertEquals(0, p3.compareTo(p2));
  }

  @Test
  public void compareEqualWeight() {
    final ProcessNumber p1 = new ProcessNumber(1, "a");
    final ProcessNumber p2 = new ProcessNumber(1, "b");
    final ProcessNumber p3 = new ProcessNumber(1, "b");

    assertEquals(-1, p1.compareTo(p2));
    assertEquals(1, p2.compareTo(p1));
    assertEquals(0, p2.compareTo(p3));
    assertEquals(0, p3.compareTo(p2));
  }
}
