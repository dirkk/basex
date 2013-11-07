package org.basex.server.replication;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.HashMap;
import java.util.Map;

import org.basex.core.*;

import com.rabbitmq.client.*;
import org.basex.core.cmd.*;
import org.basex.data.MetaData;
import org.basex.io.in.BlockInput;

import static org.basex.data.DataText.DBNAME;
import static org.basex.data.DataText.DBPATH;

/**
 * Subscriber and slave within a replica set. This instance is non-writable
 * as all updating queries have to be done by the master. The master will
 * then propagate all changes to the connected slaves.
 * 
 * A subscriber is connected to a RabbitMQ message broker.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class Subscriber extends Distributor implements Runnable {
  /** Incoming messages. */
  private QueueingConsumer consumer;
  /** Queue name. */
  private String queueName;
  
  /**
   * Constructor.
   * @param c database context
   * @param a message queue URI
   * @param t replica set name
   * @throws BaseXException connection exception
   */
  public Subscriber(final Context c, final String a, final String t) throws BaseXException {
    super(c, a, t);
    try {
      connect();
    } catch(Exception e) {
      context.log.writeError(e);
      throw new BaseXException(e);
    }
  }
  
  /**
   * Establish a connect to the message broker.
   * @throws IOException I/O exception
   * @throws URISyntaxException exception
   * @throws NoSuchAlgorithmException exception
   * @throws KeyManagementException exception
   */
  private void connect() throws IOException, KeyManagementException, NoSuchAlgorithmException, URISyntaxException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUri(addr);
    connection = factory.newConnection();
    channel = connection.createChannel();

    channel.exchangeDeclarePassive(EXCHANGE_NAME);
    queueName = channel.queueDeclare("BaseXRepl" + context.toString(),
        true, true, true, null).getQueue();
    channel.queuePurge(queueName);
    channel.basicQos(10);
    channel.queueBind(queueName, EXCHANGE_NAME, topic);

    consumer = new QueueingConsumer(channel);
    channel.basicConsume(queueName, false, consumer);
  }
  
  /**
   * Close the channel and connection.
   */
  public void close() {
    try {
      channel.queueDelete(queueName);
      connection.close();
    } catch(IOException e) {
      context.log.writeError(e);
    }
  }

  private void readMessage(final byte[] data) throws BaseXException {
    BlockInput bi = new BlockInput(data);
    byte type = bi.readByte();

    if (type == TYPE.Document.getValue()) {
      readDocumentMessage(bi);
    } else if (type == TYPE.Database.getValue()) {
      readDatabaseMessage(bi);
    } else if (type == TYPE.DatabaseDelete.getValue()) {
      readDatabaseDeleteMessage(bi);
    } else if(type == TYPE.DocumentDelete.getValue()) {
      readDocumentDeleteMessage(bi);
    }
  }

  private void readDocumentMessage(final BlockInput bi) throws BaseXException {
    final String uri = bi.readString();
    final int s = uri.indexOf("/");
    final String db = uri.substring(0, s);
    final String path = uri.substring(s + 1);
    final String content;
    try {
      content  = new String(bi.readBlock(), "UTF-8");

      // execute
      new Check(db).execute(context);
      new Replace(path, content).execute(context);
      new Close().execute(context);
    } catch (UnsupportedEncodingException e) {
      // TODO
      e.printStackTrace();
    }
  }

  private void readDocumentDeleteMessage(final BlockInput bi) throws BaseXException {
    final String path = bi.readString();

    new Delete(path).execute(context);
  }

  private void readDatabaseMessage(final BlockInput bi) throws BaseXException {
    // meta information
    MetaData meta = new MetaData(new Prop());

    byte amount = bi.readByte();
    for (byte i = 0; i < amount; ++i) {
      String k = bi.readString();
      String v = bi.readString();

      if(k.equals(DBNAME))         meta.name   = v;
    }

    // read in all XML documents
    Map<String, String> docs = new HashMap<String, String>();
    final int numberXML = bi.readInt();
    for (int i = 0; i < numberXML; ++i) {
      final String name = bi.readString();
      final byte[] c = bi.readBlock();
      try {
        docs.put(name, new String(c, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        // TODO
        e.printStackTrace();
      }
    }

    // execute
    // drop old database, create
    new DropDB(meta.name).execute(context);
    new CreateDB(meta.name).execute(context);

    for (String name : docs.keySet()) {
      new Add(meta.name + "/" + name, docs.get(name));
    }

    new Close().execute(context);
  }

  private void readDatabaseDeleteMessage(final BlockInput bi) throws BaseXException {
    final String name = bi.readString();

    new DropDB(name).execute(context);
  }

  @Override
  public void run() {
    try {
      while (true) {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        byte[] body = delivery.getBody();
        readMessage(body);
        
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    } catch (Exception e) {
      context.log.writeError(e);
    }
  }
}
