package org.basex.server.replication;

import java.io.*;

import org.basex.core.*;

import com.rabbitmq.client.*;

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
   * @param m object to publish
   */
  public void publish(Message m) {
    try {
      byte[] send = m.serialize();
      channel.basicPublish(EXCHANGE_NAME, topic, null, send);
    } catch(IOException e) {
      context.log.writeError(e);
      System.err.println(e.getMessage());
    }
  }
}