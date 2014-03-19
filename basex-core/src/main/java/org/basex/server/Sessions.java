package org.basex.server;

import org.basex.util.TokenBuilder;
import org.basex.util.list.StringList;

import java.util.concurrent.CopyOnWriteArrayList;

import static org.basex.core.Text.*;

/**
 * This class organizes all currently opened database sessions.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
public final class Sessions extends CopyOnWriteArrayList<AListener> {
  /**
   * Returns information about the currently opened sessions.
   * @return data reference
   */
  public synchronized String info() {
    final TokenBuilder tb = new TokenBuilder();
    tb.addExt(SESSIONS_X, size()).add(size() == 0 ? DOT : COL);

    final StringList sl = new StringList();
    for(final AListener sp : this) {
      sl.add(sp.dbCtx().user.name + ' ' + sp);
    }
    for(final String sp : sl.sort(true)) tb.add(NL).add(LI).add(sp);
    return tb.toString();
  }
}
