package org.basex.server.replication;

import java.io.*;
import java.net.*;

import com.rabbitmq.client.*;

public class Subscriber implements Runnable {
  /** RabbitMQ address. */
  private final InetSocketAddress addr;
  /** Connection to the message broker. */
  private Connection connection;
  /** Channel to the message broker. */
  private Channel channel;
  /** Incoming messages. */
  private QueueingConsumer consumer;
  /** Test. */
  private static final String EXCHANGE_NAME = "test";
  
  /**
   * Constructor.
   * @param a message queue address
   */
  public Subscriber(final InetSocketAddress a) {
    addr = a;
  }
  
  /**
   * Establish a connect to the message broker.
   * @throws IOException I/O exception
   */
  private void connect() throws IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    connection = factory.newConnection();
    channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    String queueName = channel.queueDeclare().getQueue();
    channel.queueBind(queueName, EXCHANGE_NAME, "");

    consumer = new QueueingConsumer(channel);
    channel.basicConsume(queueName, true, consumer);
  }
  
  /**
   * Close the channel and connection.
   */
  private void close() {
    try {
      channel.close();
      connection.close();
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    try {
      connect();
      
      while (true) {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        String message = new String(delivery.getBody());

        System.out.println(" [x] Received '" + message + "'");
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
    } finally {
      close();
    }
  }
}
