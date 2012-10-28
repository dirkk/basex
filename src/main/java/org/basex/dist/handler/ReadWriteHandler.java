package org.basex.dist.handler;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import org.basex.dist.*;

/**
 * Uses non-blocking operations to read and write from/to a socket. Can
 * communicate with another peer in the cluster network.
 * 
 * @author Dirk Kirsten
 *
 */
public class ReadWriteHandler {
  /** The socket where r/w operations are executed. */
  protected SocketChannel channel;
  /** The selector to register the associated channel to. */
  protected Selector selector;
  /** The cluster peer this socket writes to and reads from. */
  protected ClusterPeer cp;
  /** Reading buffer. */
  protected ByteBuffer inBuffer;
  /** Writing buffer, holds just one packet TODO */
  protected ByteBuffer outBuffer;
  /** Size of the buffer used to reconstrut the packet. Should be big 
   *  enough to hold an entire packet. */
  private final static int BUFFER_SIZE = 65536;
  /** Holds a message that is not fully assembled. */
  private byte[] packetBuffer = new byte[BUFFER_SIZE];
  /** Write position on the packetBuffer. */
  private int posPacketBuffer = 0;
  
  /**
   * Default constructor
   * 
   * @param channel Socket channel
   * @param selector The selector to operate on
   * @param cp The cluster peer this socket writes to/reads from.
   * @throws IOException Could not open buffers
   */
  @SuppressWarnings("hiding")
  public ReadWriteHandler(final SocketChannel channel, final Selector selector,
      final ClusterPeer cp)
      throws IOException {
    this.channel = channel;
    this.selector = selector;
    this.cp = cp;
    
    // create the input buffer
    inBuffer = ByteBuffer.allocateDirect(channel.socket().getReceiveBufferSize());
    outBuffer = null;
    
    // register read interest
    channel.register(selector, SelectionKey.OP_READ, this);
  }
  
  /**
   * Reads from the socket channel into the internal buffer.
   */
  public void handleRead() {
    try {      
      // Reads from the socket
      int readBytes = channel.read(inBuffer);  
      if (readBytes == -1) {
        // end of stream
        close();
        return;
      }
      
      // Nothing else to be read?
      if (readBytes == 0) {
        changeInterestSet(SelectionKey.OP_READ);
        return;
      }
      
      // There is some data in the buffer, so proccess it
      inBuffer.flip();
      // convert it into a packet
      ByteBuffer packet = decode(inBuffer);
      
      // A packet may or may not have been fully assembled, depending
      // on the data available in the buffer
      if (packet == null) {      
        // Partial packet received
        inBuffer.clear();
        changeInterestSet(SelectionKey.OP_READ);
      } else {      
        // A full packet was reassembled.
        cp.packetArrived(packet);
        // The inBuffer might still have some data left. Perhaps
        // the beginning of another packet. So don't clear it. Next
        // time reading is activated, we start by processing the inBuffer
        // again.
      }
    } catch (IOException ex) {
      close();
    }
  }
  
  /**
   * Adds the given interest set to the current interest set, so it
   * is an addition in registered interests for this channel.
   *
   * @param ops the added interest
   */
  private void changeInterestSet(int ops) {
    SelectionKey sk = channel.keyFor(selector);
    sk.interestOps(sk.interestOps() | ops);
  }
  
  /**
   * Sets a new interest set for this channel, overwriting the old set.
   * 
   * @param ops The new interest set
   */
  private void setInterestSet(int ops) {
    channel.keyFor(selector).interestOps(ops);
  }
  
  /**
   * Decodes data from the input buffer and returns a fully assembled packet.
   * Returns null, if the packet is not yet fully reconstructed. Can raise an
   * error if the internal buffer is overflow, most likely due to a corrupt
   * package.
   * 
   * @param socketBuffer buffer to read from.
   * @return A fully reassembled packet or null, if not yet complete
   * @throws IOException internal buffer was exceeded, most likely a corrupt
   * package was read
   */
  private ByteBuffer decode(ByteBuffer socketBuffer) throws IOException {    
    // Reads until the buffer is empty or until a packet
    // is fully reassembled.
    while (socketBuffer.hasRemaining()) {
      try {
        // the first two bytes of a packet determine the length
        if (posPacketBuffer < 2) {
          // packet length has not been read
          packetBuffer[posPacketBuffer++] = socketBuffer.get();
        } else {
          // we know the expected packet length, so use it
          int lengthPacket = ByteBuffer.wrap(packetBuffer, 0, 2).getShort(0);
          socketBuffer.get(packetBuffer, 0,
              Math.min(lengthPacket, socketBuffer.remaining()));
        }
      } catch (IndexOutOfBoundsException e) {
        // The buffer has a fixed limit. If this limit is reached, then
        // most likely the packet that is being read is corrupt.
        e.printStackTrace();
        throw new IOException(
            "Packet too big. Maximum size allowed: " + BUFFER_SIZE + " bytes.");
      }

      // The first two bytes of the packet determine the length of the packet
      if (posPacketBuffer >= 2) {
        // The current packet is fully reassembled. Return it
        byte[] newBuffer = new byte[posPacketBuffer];
        System.arraycopy(packetBuffer, 0, newBuffer, 0, posPacketBuffer);
        ByteBuffer packetByteBuffer = ByteBuffer.wrap(newBuffer);        
        posPacketBuffer = 0;

        return packetByteBuffer;
      }
    }
    // No packet was reassembled. There is not enough data. Wait
    // for more data to arrive.
    return null;
  }
  
  /**
   * Close this handler. Closes the associated socket channel.
   */
  public void close() {
    try {
      channel.close();
    } catch (IOException e) {}
  }
}
