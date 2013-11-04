package org.basex.server.replication;

import org.basex.core.*;
import org.basex.io.in.*;

/**
 * Abstract message class to send from a master instance to connected
 * slaves within a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public abstract class Message {
  /**
   * Parse the incoming data and construct a message. The message type
   * is given as the first byte value of the incoming array.
   * @param data message data
   * @return constructed message
   */
  public static Message parse(byte[] data) {
    BlockInput bi = new BlockInput(data);
    byte type = bi.readByte();
    
    if (type == DocumentMessage.TYPE) {
      return new DocumentMessage(bi);
    }
    else if (type == DatabaseMessage.TYPE) {
      return new DatabaseMessage(bi);
    }
    
    return null;
  }
  /**
   * Serialize the message and returns it as byte array.
   * 
   * @return serialized message
   */
  public abstract byte[] serialize();
  /**
   * Store the information within the message in the given
   * database context.
   *
   * @param context database context
   * @throws BaseXException exception
   */
  public abstract void save(final Context context) throws BaseXException;
}
