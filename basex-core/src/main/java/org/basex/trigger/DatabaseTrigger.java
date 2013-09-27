package org.basex.trigger;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface DatabaseTrigger {
  public void afterCreateDb(final String name);
  public void afterAlter(final String source, final String target);
  public void afterDrop(final String name);
  public void afterCopy(final String source, final String target);
}
