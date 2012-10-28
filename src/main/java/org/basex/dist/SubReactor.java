package org.basex.dist;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

/**
 * Sub reactor.
 * 
 * @author Dirk Kirsten
 *
 */
public class SubReactor implements Runnable {
  /** Selector. */
  private Selector selector;
  /** is still alive. */
  private boolean isRunning;
  
  /**
   * Default constructor.
   * 
   * @param s Selector to listen to.
   */
  public SubReactor(final Selector s) {
    this.selector = s;
    this.isRunning = true;
  }

  @Override
  public void run() {
    while(isRunning) {
      try {
        selector.select();
        Set<SelectionKey> selected = selector.selectedKeys();
        System.out.println("Got an event.");
        
        Iterator<SelectionKey> it = selected.iterator();
        while (it.hasNext()) {
          // dispatch the event
          SelectionKey sk = it.next();
          Runnable r = (Runnable) sk.attachment();
          if (r != null) {
            r.run();
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
