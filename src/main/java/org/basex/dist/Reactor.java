package org.basex.dist;

import java.io.*;
import java.net.*;
import java.nio.channels.*;

import org.basex.dist.handler.*;
/**
 * Implements the reactor pattern to do networking for the distributed
 * BaseX version. Channels can register themselve at the reactor and will
 * be notified if certain I/O events are dispatched.
 * 
 * @author Dirk Kirsten
 *
 */
public class Reactor {
  /** TODO: should not be a constant, but instead be editable by the user. */
  private final static int NUMBER_SELECTORS = 20;
  /** Server host and port address. */
  protected final InetSocketAddress serverAddress;
  /** Server listening channel. */
  protected ServerSocketChannel serverChannel;
  /** Selector. */
  protected final Selector selectors[];
  /** Next selector to be used. */
  protected int nextSelector;
  /** Controlling network peer. */
  protected NetworkPeer peer;
  
  /**
   * Default constructor.
   * 
   * @param host listening host
   * @param port listening port
   * @param p commanding network peer
   * @throws IOException the listening socket could not be opened or bind
   */
  public Reactor(final String host, final int port,
      final NetworkPeer p) throws IOException {
    this(new InetSocketAddress(host, port), p);
  }
  
  /**
   * Default constructor.
   * 
   * @param host listening host
   * @param port listening port
   * @param p commanding network peer
   * @throws IOException the listening socket could not be opened or bind
   */
  public Reactor(final InetAddress host, final int port,
      final NetworkPeer p) throws IOException {
    this(new InetSocketAddress(host, port), p);
  }
  
  /**
   * Default constructor.
   * 
   * @param address listening host and port
   * @param p commanding network peer
   * @throws IOException the listening socket could not be opened or bind
   */
  public Reactor(final InetSocketAddress address, final NetworkPeer p
      ) throws IOException {
    this.peer = p;
    serverAddress = address;
    
    selectors = new Selector[NUMBER_SELECTORS];
    nextSelector = 0;
    for (int i = 0; i < NUMBER_SELECTORS; ++i) {
      selectors[i] = Selector.open();
      SubReactor sr = new SubReactor(selectors[i]);
      new Thread(sr).start();
    }
    
    /* New acceptor which will listen for new incoming connections. There is
     * just a need for a single acceptor.
     */

    serverChannel = ServerSocketChannel.open();
    serverChannel.configureBlocking(false);
    serverChannel.socket().bind(serverAddress);
    serverChannel.register(getNextSelector(), SelectionKey.OP_ACCEPT,
        new AcceptorHandler(p));
  }
  
  /**
   * Get Server channel socket.
   * @return server socket channel
   */
  public ServerSocketChannel getServerChannel() {
    return this.serverChannel;
  }
  
  /**
   * Returns the next selector in a very simple round-robin distribution.
   * @return Selector to be used
   */
  public Selector getNextSelector() {
    Selector s = selectors[nextSelector];
    
    if (++nextSelector >= selectors.length) {
      nextSelector = 0;
    }
    
    return s;
  }
  
  /**
   * Returns the listening server socket.
   *
   * @return The socket address of the server
   */
  public InetSocketAddress getSocketAddress() {
    return serverAddress;
  }
  
  /**
   * Gracefully close down the network infrastructure for this peer.
   * @throws IOException Server socket could not be closed gracefully.
   */
  public void close() throws IOException {
    serverChannel.close();
  }

}
