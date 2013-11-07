package org.basex.server.replication;

import java.io.*;

import org.basex.core.*;

import com.rabbitmq.client.*;
import org.basex.data.Data;
import org.basex.data.MetaData;
import org.basex.io.out.ArrayOutput;
import org.basex.io.out.BlockOutput;
import org.basex.io.serial.Serializer;
import org.basex.query.value.node.DBNode;
import org.basex.util.Token;
import org.basex.util.list.IntList;

import static org.basex.core.Text.R_NOT_MASTER;
import static org.basex.data.DataText.DBNAME;

/**
 * Publisher of changes and master of a replica set. This takes care of all
 * updating queries within a replica set.
 *
 * A publisher is connected to a RabbitMQ message broker.
 * 
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class Publisher extends Distributor {  
  /**
   * Constructor
   * @param c database context
   * @param a message queue address
   * @param t replica set name
   * @throws BaseXException connection exception
   */
  public Publisher(final Context c, final String a, final String t) throws BaseXException {
    super(c, a, t);
    start();
  }
  
  /**
   * Close the channel and connection.
   */
  public void close() {
    try {
      // deletes the exchanges and automatically closes the channel
      channel.exchangeDelete(EXCHANGE_NAME);
      connection.close();
    } catch(IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Connect to the RabbitMQ message broker.
   * @throws BaseXException connection exception
   */
  public void start() throws BaseXException {
    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setUri(addr);
      connection = factory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare(EXCHANGE_NAME, "topic", false);
    } catch (Exception e) {
      context.log.writeError(e.getCause());
      throw new BaseXException(e);
    }
  }
  
  /**
   * Publishes a message to the message exchange.
   *
   * @param b byte array to publish
   */
  public void publish(byte[] b) {
    try {
      channel.basicPublish(EXCHANGE_NAME, topic, null, b);
    } catch(IOException e) {
      context.log.writeError(e);
      System.err.println(e.getMessage());
    }
  }


  /**
   * Replicate the complete database
   *
   * @param data data instance to replicate
   * @throws BaseXException exception
   */
  public void replicateDatabase(final Data data) throws BaseXException {
    BlockOutput out = new BlockOutput();

    // write the type
    out.writeByte(TYPE.Database.getValue());
    // write metadata as pairs of key and value
    MetaData meta = data.meta;
    out.writeByte((byte) 1);
    out.writePair(DBNAME, meta.name);

    // serialize all XML documents
    final IntList il = data.resources.docs();
    final int is = il.size();
    out.writeInt(is);

    for(int i = 0; i < is; i++) {
      final int pre = il.get(i);
      String name = Token.string(data.text(pre, true));
      out.writeString(name);

      byte[] b;
      try {
        b = serializeDoc(new DBNode(data, pre));
      } catch (IOException e) {
        throw new BaseXException(e);
      }
      out.writeBlock(b);
    }

    publish(out.toArray());
  }

  public void replicateDatabaseDelete(final String name) {
    BlockOutput out = new BlockOutput();

    // write the type
    out.writeByte(TYPE.DatabaseDelete.getValue());

    out.writeString(name);

    publish(out.toArray());
  }

  /**
   * Serializes a document stored in the database. It will encode the message
   * using prepending length fields. It send the whole base URI (include
   * database and file name) and the UTF-8 serialized document.
   *
   * @return byte array holding the message.
   */
  public void replicateDocument(final DBNode node) throws BaseXException {
    final byte[] uri = node.baseURI();
    BlockOutput bo = new BlockOutput();

    // write the type
    bo.writeByte(TYPE.Document.getValue());

    bo.writeBlock(uri);

    byte[] b;
    try {
      b = serializeDoc(node);
    } catch (IOException e) {
      throw new BaseXException(e);
    }
    bo.writeBlock(b);

    publish(bo.toArray());
  }
  public void replicateDocumentDelete(final String path) {
    BlockOutput out = new BlockOutput();

    // write the type
    out.writeByte(TYPE.DocumentDelete.getValue());

    out.writeString(path);

    publish(out.toArray());
  }


  /**
   * Writes a document to the given output stream.
   *
   * @param node database node
   * @throws java.io.IOException I/O exception
   */
  private byte[] serializeDoc(final DBNode node) throws IOException {
    final ArrayOutput ao = new ArrayOutput();
    final Serializer ser = Serializer.get(ao, null);
    ser.serialize(node);
    ser.close();

    byte[] t = ao.toArray();
    ao.close();
    return t;
  }
}