package org.basex.server.replication;

import java.io.*;
import java.nio.*;
import java.util.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.data.*;
import org.basex.io.serial.*;
import org.basex.query.value.node.*;

/**
 * A document holding message. This class is send from a master to all
 * connected slaves, if the document has been updated. The document is
 * serialized as UTF-8.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class DocumentMessage {
  /** Database node. */
  private final DBNode node;
  /** Base URI of the document. */
  private final byte[] uri;
  /** Document content. */
  private byte[] content;
  
  /**
   * Default constructor.
   *
   * @param data data structure
   * @param pre pre value of the document
   */
  public DocumentMessage(final Data data, final int pre) {
    node = new DBNode(data, pre);
    uri = node.baseURI();
  }
  
  /**
   * Default constructor.
   *
   * @param u base URI
   * @param c serialized file content
   */
  public DocumentMessage(final byte[] u, final byte[] c) {
    node = null;
    uri = u;
    content = c;
  }
  
  /**
   * Serialize a document file.
   * 
   * @return serialized document as byte array
   * @throws IOException I/O exception
   */
  public byte[] serializeDocument() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    // serialize file
    final Serializer ser = Serializer.get(bos, null);
    ser.serialize(node);
    ser.close();
    bos.close();
    
    content = bos.toByteArray();
    return content;
  }

  /**
   * Serializes a document stored in the database. It will encode the message
   * using prepending length fields. It send the whole base URI (include
   * database and file name) and the UTF-8 serialized document.
   * 
   * @return byte array holding the message.
   */
  public byte[] serialize() {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      bos.write(0x00);

      bos.write(ByteBuffer.allocate(4).putInt(uri.length).array());
      bos.write(uri);
      
      serializeDocument();
      bos.write(ByteBuffer.allocate(4).putInt(content.length).array());
      bos.write(content);
      
      return bos.toByteArray();
    } catch(IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Returns the base URI of the document.
   * @return base URI
   */
  public String getBaseURI() {
    try {
      return new String(uri, "UTF-8");
    } catch(UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Returns the database name of this document.
   * @return database identifier
   */
  public String getDatabase() {
    String u = getBaseURI();
    int s = u.indexOf("/");
    return u.substring(0, s);
  }
  
  /**
   * Returns the document path.
   * @return database path
   */
  public String getPath() {
    String u = getBaseURI();
    int s = u.indexOf("/");
    return u.substring(s + 1);
  }
  
  /**
   * Get the file content as UTF-8 encoded string.
   * @return file content
   */
  public String getContent() {
    try {
      return new String(content, "UTF-8");
    } catch(UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Save this document in the specified database context.
   * 
   * @param context database context
   * @throws BaseXException exception
   */
  public void saveDocument(final Context context) throws BaseXException {
    try {
      new Open(getDatabase()).execute(context);
    } catch (Exception e) {
      new CreateDB(getDatabase()).execute(context);
    }
    new Replace(getPath(), getContent()).execute(context);
    new Close().equals(context);
  }
  
  /**
   * Create a document message object from a incoming byte array.
   * 
   * @param data incoming data packet
   * @return whole message
   */
  public static DocumentMessage construct(byte[] data) {
    if (data[0] == 0) {
      int i, length, start, end;
      i = 1;
      length = ByteBuffer.wrap(Arrays.copyOfRange(data, i, i + 4)).getInt();
      start = i + 4;
      end = 4 + length;
      byte[] uri = Arrays.copyOfRange(data, start, end + 1);
      
      i = end + 1;
      length = ByteBuffer.wrap(Arrays.copyOfRange(data, i, i + 4)).getInt();
      start = i + 4;
      end = start + length;
      byte[] content = Arrays.copyOfRange(data, start, end);
      
      return new DocumentMessage(uri, content);
    }
    
    return null;
  }
}
