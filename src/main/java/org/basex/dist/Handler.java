package org.basex.dist;

import java.io.*;
import java.nio.channels.*;
import java.util.concurrent.*;

/**
 * Handles the reading and/or writing of network I/O for the distributed
 * BaseX version.
 *
 * @author Dirk Kirsten
 *
 */
public class Handler implements Runnable {
  /** Socket channel. */
  protected final SocketChannel channel;
  /** Selection Key. */
  protected final SelectionKey sk;
  /** Constants for I/O state. */
  protected final static int READING = 0, WRITING = 1, PROCESSING = 2;
  /** I/O state of this handler. */
  protected int state = READING;
  /** Thread pool. */
  protected ThreadPoolExecutor pool;
  /** Jobs waiting to be executed. */
  protected final ArrayBlockingQueue<Runnable> workQueue;
  
  
  /**
   * Default constructor.
   * 
   * @param selector selector to register this handler to.
   * @param ch channel to listen to
   * @throws IOException Could not register this channel
   */
  public Handler(Selector selector, SocketChannel ch) throws IOException {
    channel = ch;
    channel.configureBlocking(false);
    
    // TODO choose other than constant 100
    workQueue = new ArrayBlockingQueue<Runnable>(100);
    /* create thread pool. */
    pool = new ThreadPoolExecutor(
        // at most twice as many threads as cores should run in parallel
        Runtime.getRuntime().availableProcessors() * 2,
        // at most 8 times as many threads as cores should be in the thread
        // pool
        Runtime.getRuntime().availableProcessors() * 8,
        // cancel any thread after 100 seconds of inactivity
        100, TimeUnit.SECONDS,
        // queue holding all the task to be executed
        workQueue);
    
    /* register this channel at the given selector for reading and
     * start the first read. */
    sk = channel.register(selector, SelectionKey.OP_READ);
    sk.attach(this);
    selector.wakeup();
  }
  
  /**
   * Read a whole packet until it is complete.
   * 
   * @throws IOException Could not read from socket.
   */
  private synchronized void read() throws IOException {
  }
  
  /**
   * Write a whole packet until it is complete.
   * 
   * @throws IOException Could not write socket.
   */
  private synchronized void write() throws IOException {
    
  }
  
  @Override
  public void run() {
    try {
      switch (state) {
        case READING:
          read();
          break;
        case WRITING:
          write();
          break;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * This one actually does all the work.
   */
  private static class Worker implements Runnable {

    @Override
    public void run() {
      // process stuff
    }
    
  }

}
