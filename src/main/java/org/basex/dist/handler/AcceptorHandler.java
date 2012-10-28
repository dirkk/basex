package org.basex.dist.handler;

import java.io.*;
import java.nio.channels.*;

import org.basex.dist.*;

/**
 * Listens and accepts new incoming connections from other peers within the
 * network. This is specific to the distributed BaseX environment and should
 * not be confused with the client/server infrastructure.
 * 
 */
public class AcceptorHandler implements Runnable {
  /** Server is running. */
  protected boolean isRunning = true;
  /** Listening for the following network peer. */
  protected NetworkPeer peer;
  
  /**
   * Default constructor.
   *
   * @param p network peer.
   */
  public AcceptorHandler(final NetworkPeer p)  {
    this.peer = p;
  }
  
  /**
   * Main cycle. The method blocks until a new connection attempt. If so,
   * it initializes a connection establishment process.
   */
  @Override
  public synchronized void run() {
    while (isRunning) {
      try {
        Reactor reactor = peer.getReactor();
        
        System.out.println("Running server on " +
            reactor.getServerChannel().getLocalAddress().toString());
        SocketChannel ch = reactor.getServerChannel().accept();
        
        if (ch != null) {
          System.out.println("Connection request from" + ch.socket().getRemoteSocketAddress().toString());
          peer.connectionEstablished(ch);
        }
        
        // re-register interest
        reactor.getServerChannel().register(reactor.getNextSelector(),
            SelectionKey.OP_ACCEPT,
            this);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
  }
}
