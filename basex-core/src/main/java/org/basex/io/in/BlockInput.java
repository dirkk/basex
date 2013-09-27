package org.basex.io.in;

import java.nio.*;
import static org.basex.util.Token.*;

/**
 * Reads a block input, a byte array prepended by its length.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class BlockInput {
  /** Backing Buffer. */
  private byte[] buffer;
  /** Current reading position. */
  private int pos;
  
  /**
   * Default constructor.
   * 
   * @param b input data
   */
  public BlockInput(final byte[] b) {
    buffer = b;
    pos = 0;
  }
  
  /**
   * Read a single byte.
   * 
   * @return byte
   */
  public byte readByte() {
    synchronized (buffer) {
      return buffer[pos++];
    }
  }


  /**
   * Read a single int value.
   *
   * @return int read value
   */
  public int readInt() {
    synchronized (buffer) {
      return (buffer[pos++] << 24) +
             ((buffer[pos++] & 0xFF) << 16) +
             ((buffer[pos++] & 0xFF) << 8) +
             (buffer[pos++] & 0xFF);
    }
  }
  
  /**
   * Read a whole block, returning the payload data.
   * @return payload
   */
  public byte[] readBlock() {
    int length = readInt();
    byte[] bl = new byte[length];
    synchronized (buffer) {
      System.arraycopy(buffer, pos, bl, 0, length);
      pos += length;
      return bl;
    }
  }

  /**
   * Read a block and convert to a string.
   * 
   * @return string
   */
  public String readString() {
    return string(readBlock());
  }
}
