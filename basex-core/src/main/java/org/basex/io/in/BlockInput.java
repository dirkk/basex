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
  private ByteBuffer buf;
  
  /**
   * Default constructor.
   * 
   * @param b input data
   */
  public BlockInput(final byte[] b) {
    buf = ByteBuffer.wrap(b);
  }
  
  /**
   * Read a single byte.
   * 
   * @return byte
   */
  public byte readByte() {
    return buf.get();
  }
  
  /**
   * Read a whole block, returning the payload data.
   * @return payload
   */
  public byte[] readBlock() {
    int length = buf.getInt();
    byte[] bl = new byte[length];
    buf.get(bl);
    return bl;
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
