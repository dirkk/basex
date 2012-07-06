package org.basex.dist;

import java.io.*;
import java.nio.channels.*;
import java.util.*;

final public class PeerSelector {
  /** Selector used for I/O multiplexing. */
  private Selector selector;
  /** Should this Selector be closed. */
  private boolean requestClose;
  /** Pending list of task to be executed by the selector. */
  private List<Runnable> pendingInvocations = new ArrayList<Runnable>(64);

  /**
   * Default constructor.
   *
   * @throws IOException Could not open the selector
   */
  public PeerSelector() throws IOException {
    selector = Selector.open();
  }

  /**
   * Closes this selector gracefully.
   */
  public void close() {
    requestClose = true;
    selector.wakeup();
  }

  /**
   * Executes all tasks queued for execution on this selector.
   *
   */
  private void doInvocations() {
    synchronized (pendingInvocations) {
      for (int i = 0; i < pendingInvocations.size(); ++i) {
        Runnable task = pendingInvocations.get(i);
        task.run();
      }
      pendingInvocations.clear();
    }
  }

  private void changeInterests(SelectionKey key, final int interestOps) {
    key.interestOps(interestOps);
  }

  public void registerClusterPeerBlocking(final SelectableChannel channel,
      final int interestOps,
      final ClusterPeer cp) {
    invoke(new Runnable() {
      @Override
      public void run() {
        try {
        registerClusterPeer(channel, interestOps, cp);
        } catch (IOException e) {
          //TODO handle properly
        }
      }
    });
  }

  /**
   * Registers a SelectableChannel with this selector. This channel will
   * start to be monitored by the selector for the set of events associated
   * with it. When an event is raised, the corresponding handler is
   * called.
   *
   * This method can be called multiple times with the same channel
   * and selector. Subsequent calls update the associated interest set
   * and selector handler to the ones given as arguments.
   *
   * @param ch The channel to be monitored.
   * @param interestOps The interests to monitor
   * @param cp ClusterPeer to register
   * @throws IOException io exception
   */
  public void registerClusterPeer(final SelectableChannel ch,
                                 final int interestOps,
                                 final ClusterPeer cp) throws IOException {
    if (!ch.isOpen()) throw new IOException("Channel is not open.");

    try {
      if (ch.isRegistered()) {
        SelectionKey key = ch.keyFor(selector);
        assert key != null : "Channel is already registered with other selector";
        key.interestOps(interestOps);
        Object previousAttach = key.attach(cp);
        assert previousAttach != null;
      } else {
        ch.configureBlocking(false);
        ch.register(selector, interestOps, cp);
      }
    } catch (Exception e) {
      IOException ioe = new IOException("Error registering channel.");
      throw ioe;
    }
  }

  /**
   * Executes the given task in the selector. This method returns
   * immediately and execution is queued until later.
   *
   * @param task The task to be executed.
   */
  public void invoke(final Runnable task) {
    synchronized (pendingInvocations) {
      pendingInvocations.add(task);
    }
    selector.wakeup();
  }

  /**
   * Executes the given task synchronously in the selector. This
   * method schedules the task, waits for its execution and only then
   * returns.
   *
   * @param task The task to be executed
   * @exception InterruptedException invoking interrupted
   */
  public void invokeSynchronous(final Runnable task)
    throws InterruptedException {
    final Object wait = new Object();

    this.invoke(new Runnable() {
      @Override
      public void run() {
        task.run();
        wait.notify();
      }
    });
    wait.wait();
  }

  /**
   * Main cycle. Processes events and dispatches them accordingly.
   *
   */
  public void run() {
    while (!requestClose) {
      doInvocations();

      Iterator<SelectionKey> it = selector.selectedKeys().iterator();
      while (it.hasNext()) {
        SelectionKey key = it.next();
        it.remove();

        // obtain the registered interests of this keys channel
        int readyOps = key.readyOps();
        // Disable the interest for the ready operations of this keys channel now
        // to prevent being raised several times
        key.interestOps(~readyOps & key.interestOps());

        ClusterPeer cp = (ClusterPeer) key.attachment();
        if (key.isAcceptable())
          cp.handleAccept();
        else if (key.isConnectable())
          cp.handleConnect();
        else if (key.isReadable())
          cp.handleRead();
        else if (key.isWritable())
          cp.handleWrite();
        
      }
    }
  }

  /**
   * Shuts down all connected keys and the selector itself.
   * Shut be used to gracefully shutdown this class.
   */
  private void cleanUp() {
    Set<SelectionKey> keys = selector.keys();
    Iterator<SelectionKey> it = keys.iterator();
    while (it.hasNext()) {
      SelectionKey key = it.next();
      try {
        key.channel().close();
      } catch (IOException e) {
        //TODO log somewhere
      }
    }

    try {
      selector.close();
    } catch (IOException e) {
      //TODO log somewhere
    }
  }
}
