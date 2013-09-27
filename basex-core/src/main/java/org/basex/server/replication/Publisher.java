package org.basex.server.replication;

import java.io.*;
import java.net.*;

import com.rabbitmq.client.*;

public class Publisher implements Runnable {
  /** RabbitMQ address. */
  private final InetSocketAddress addr;
  /** Connection to the message broker. */
  private Connection connection;
  /** Channel to the message broker. */
  private Channel channel;
  /** Test. */
  private static final String EXCHANGE_NAME = "test";
  
  /**
   * Constructor
   * @param a message queue address
   */
  public Publisher(final InetSocketAddress a) {
    addr = a;
  }
  
  /**
   * Connect to the RabbitMQ message broker.
   * 
   * @throws IOException I/O exception
   */
  private void connect() throws IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(addr.getHostString());
    factory.setPort(addr.getPort());

    connection = factory.newConnection();
    channel = connection.createChannel();
    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
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

      // TODO just testing
      try {
        Thread.sleep(5000);
      } catch(InterruptedException e) {
        e.printStackTrace();
      }
      for (int i = 0; i < 10; ++i) {
        String message = "mymessage: " + i;
  
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
        try {
          Thread.sleep(500);
        } catch(InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } finally {
      close();
    }
  }

}