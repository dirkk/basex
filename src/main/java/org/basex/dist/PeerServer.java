package org.basex.dist;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.*;

import org.basex.util.*;

/**
 * Each peer within the network consists of a Server part to process
 * incoming requests from other connected peers.
 *
 * @author Dirk Kirsten
 */
public class PeerServer implements Runnable {
  /** Reference to this peer. */
  private final NetworkPeer peer;
  /** Server socket channel. */
  private ServerSocketChannel server;
  /** Port and host name to listen for incoming requests. */
  private final InetSocketAddress socketAddress;
  /** Packet selector for nio. */
  private Selector selector;

  /**
   * Default constructor.
   *
   * @param np The peer this server is connected to
   * @param s Port and host name as SocketAddress to listen to for incoming requests
   */
  public PeerServer(final NetworkPeer np, final InetSocketAddress s) {
    peer = np;
    socketAddress = s;
  }

  /**
   * Initializes the server socket.
   *
   * @throws IOException Socket could not be bound
   */
  private void init() throws IOException {
    // Create and open the server socket channel from a factory
    server = ServerSocketChannel.open();
    // use non-blocking I/O
    server.configureBlocking(false);
    //listen to the given port and host name
    server.socket().bind(socketAddress);
    // Create the selector for nio
    selector = Selector.open();
    // Register the selector to the socket channel
    server.register(selector, SelectionKey.OP_ACCEPT);
  }

  @Override
  public void run() {
    try {
      init();

      while(true) {
        // wait for events, this method is blocking
        selector.select();

        // Get all keys which are in a state to be processed
        Set<SelectionKey> keys = selector.selectedKeys();
        Iterator<SelectionKey> it = keys.iterator();

        while(it.hasNext()) {
          SelectionKey key = it.next();

          // a key has to be removed when processed
          it.remove();

          // a peer attempts a connection request
          if(key.isAcceptable()) {
            SocketChannel client = server.accept();
            if (client == null)
              continue;

            // use non-blocking I/O
            client.configureBlocking(false);

            // register for reading and writing
            client.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
          } else if(key.isReadable()) {

          }
        }
      }
    } catch(IOException e) {
      Util.message(e); // TODO better error handling
    }
  }

}
