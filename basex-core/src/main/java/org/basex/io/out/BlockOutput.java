package org.basex.io.out;

import java.util.Arrays;

import static org.basex.util.Token.token;

/**
 * Outputs byte data, prepending with the length of the following block.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class BlockOutput {
  /** Backing Buffer. */
  private byte[] buffer;
  
  /**
   * Default constructor.
   */
  public BlockOutput() {
    buffer = new byte[0];
  }

  /**
   * Write a single byte.
   * @param b byte to write
   */
  public void writeByte(final byte b) {
    byte[] a = new byte[1];
    a[0] = b;
    add(a);
  }

  /**
   * Write an integer. In Java an integer has 32bit, so 4 bytes.
   * @param i int to write
   */
  public void writeInt(final int i) {
    byte[] a = new byte[] {
            (byte) (i >>> 24),
            (byte) (i >>> 16),
            (byte) (i >>> 8),
            (byte) i
    };

    add(a);
  }
  
  /**
   * Write a byte array.
   * @param bl byte array to write
   */
  public void writeBlock(final byte[] bl) {
    writeInt(bl.length);
    add(bl);
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
   * Add the given byte array to the end of the byte buffer. Creates a new
   * array and stores this as byte bufer.
   *
   * @param a append to buffer
   */
  private void add(final byte[] a) {
    synchronized(buffer) {
      final byte[] tmp1 = buffer;

      buffer = Arrays.copyOf(tmp1, tmp1.length + a.length);
      System.arraycopy(a, 0, buffer, tmp1.length, a.length);
    }
  }
  
  /**
   * Converts the buffer to a byte array.
   * 
   * @return byte array.
   */
  public byte[] toArray() {
    return buffer;
  }
}
