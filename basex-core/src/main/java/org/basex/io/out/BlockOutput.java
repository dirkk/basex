package org.basex.io.out;

import java.nio.*;
import static org.basex.util.Token.*;

/**
 * Outputs byte data, prepending with the length of the following block.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class BlockOutput {
  /** Backing Buffer. */
  private ByteBuffer buf;
  
  /**
   * Default constructor.
   */
  public BlockOutput() {
    buf = ByteBuffer.allocate(1024);
  }
  
  /**
   * Write a single byte
   * @param b byte to write
   */
  public void writeByte(final byte b) {
    buf.put(b);
  }
  
  /**
   * Write a byte array.
   * @param bl byte array to write
   */
  public void writeBlock(final byte[] bl) {
    buf.putInt(bl.length);
    buf.put(bl);
  }
  
  /**
   * Write a string.
   * @param s string to write
   */
  public void writeString(final String s) {
    writeBlock(token(s));
  }
  
  /**
   * Write two string.
   * @param s1 first string to write
   * @param s2 second string to write
   */
  public void writePair(final String s1, final String s2) {
    writeString(s1);
    writeString(s2);
  }
  
  /**
   * Converts the buffer to a byte array.
   * 
   * @return byte array.
   */
  public byte[] toArray() {
    byte[] t = new byte[buf.position()];
    buf.rewind();
    buf.get(t);
    return t;
  }
}
