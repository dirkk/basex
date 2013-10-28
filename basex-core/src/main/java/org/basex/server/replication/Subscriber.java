package org.basex.server.replication;

import java.io.*;
import java.net.*;
import java.security.*;

import org.basex.core.*;

import com.rabbitmq.client.*;

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

  @Override
  public void run() {
    try {
      while (true) {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        byte[] body = delivery.getBody();
        DocumentMessage dm = DocumentMessage.construct(body);
        
        dm.saveDocument(context);
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    } catch (Exception e) {
      context.log.writeError(e);
    }
  }
}
