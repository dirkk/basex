package org.basex.server.replication;

import java.io.*;
import java.nio.*;
import java.util.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.data.*;
import org.basex.io.in.*;
import org.basex.io.serial.*;
import org.basex.query.value.node.*;

/**
 * A message, containing a whole database. This class is send from a master to all
 * connected slaves if necessary.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class DocumentMessage extends Message {
  /** Database node. */
  private final DBNode node;
  /** Base URI of the document. */
  private final byte[] uri;
  /** Document content. */
  private byte[] content;
  /** Type value. */
  public static byte TYPE = 0x00;
  
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
   * Constructor.
   * Create a document message object from a incoming byte array.
   * 
   * @param bi block input
   */
  public DocumentMessage(final BlockInput bi) {
    node = null;
    
    // TODO
    ByteBuffer bb = ByteBuffer.wrap(bi.readBlock());
    // type byte
    bb.get();
    
    // uri
    int length = bb.getInt();
    uri = new byte[length];
    bb.get(uri, 0, length);
    
    // content
    length = bb.getInt();
    content = new byte[length];
    bb.get(content, 0, length);
  }

  /**
   * Serialize a document file.
   * 
   * @return serialized document as byte array
   * @throws IOException I/O exception
   */
  private byte[] serializeDocument() throws IOException {
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
  @Override
  public byte[] serialize() {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();

      serializeDocument();
      
      ByteBuffer bb = ByteBuffer.allocate(9 + uri.length + content.length);
      bb.put(DocumentMessage.TYPE);

      bb.putInt(uri.length);
      bb.put(uri);
      
      bb.putInt(content.length);
      bb.put(content);
      
      bos.write(Arrays.copyOf(bb.array(), bb.position()));
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
  @Override
  public void save(final Context context) throws BaseXException {
    new Check(getDatabase()).execute(context);
    new Replace(getPath(), getContent()).execute(context);
    new Close().equals(context);
  }
}
