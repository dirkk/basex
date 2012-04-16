package org.basex.query.util;

import static org.basex.query.QueryText.*;

import java.util.*;

import org.basex.query.*;
import org.basex.query.item.*;
import org.basex.util.*;
import org.basex.util.list.*;

/**
 * Annotations.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class Ann extends ElementList {
  /** Annotation "private". */
  public static final QNm PRIVATE = new QNm(Token.token(QueryText.PRIVATE), FNURI);
  /** Annotation "public". */
  public static final QNm PUBLIC = new QNm(Token.token(QueryText.PUBLIC), FNURI);
  /** Annotation "updating". */
  public static final QNm UPDATING = new QNm(Token.token(QueryText.UPDATING), FNURI);

  /** QNames. */
  public QNm[] names = new QNm[1];
  /** Values. */
  public Value[] values = new Value[1];

  /**
   * Adds a QName/value pair.
   * @param name QName
   * @param value value
   * @return success flag
   */
  public boolean add(final QNm name, final Value value) {
    // create new entry
    if(size == names.length) {
      final int s = newSize();
      names = Arrays.copyOf(names, s);
      values = Arrays.copyOf(values, s);
    }
    names[size] = name;
    values[size] = value;
    size++;
    return true;
  }

  /**
   * Checks if the specified element is found in the list.
   * @param e element to be found
   * @return result of check
   */
  public boolean contains(final QNm e) {
    for(int i = 0; i < size; ++i) if(names[i].eq(e)) return true;
    return false;
  }

  /**
   * Returns the values for the specified name.
   * @param e element to be found
   * @return values
   */
  public Value[] values(final QNm e) {
    final ArrayList<Value> al = new ArrayList<Value>();
    for(int i = 0; i < size; ++i) {
      if(names[i].eq(e)) al.add(values[i]);
    }
    return al.toArray(new Value[al.size()]);
  }

  @Override
  public String toString() {
    final TokenBuilder tb = new TokenBuilder();
    for(int i = 0; i < size; ++i) {
      tb.add('%').add(names[i].string());
      final long s = values[i].size();
      if(s != 0) {
        tb.add('(');
        for(int a = 0; a < s; a++) {
          if(a != 0) tb.add(',');
          tb.add(values[i].itemAt(a).toString());
        }
        tb.add(')');
      }
      tb.add(' ');
    }
    return tb.toString();
  }
}
