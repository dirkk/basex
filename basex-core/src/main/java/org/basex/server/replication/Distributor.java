package org.basex.server.replication;

import org.basex.core.*;

import com.rabbitmq.client.*;

/**
 * Base class for the publisher and subscriber of a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public abstract class Distributor {
  /** RabbitMQ address. */
  protected final String addr;
  /** Database context. */
  protected final Context context;
  /** Connection to the message broker. */
  protected Connection connection;
  /** Channel to the message broker. */
  protected Channel channel;
  /** Exchange name. */
  protected static final String EXCHANGE_NAME = "BaseXRepl";
  /** Replica set name. Topic of the exchange. */
  protected final String topic;
  /** Message types. */
  protected static enum TYPE {
    Document((byte) 0x00), Database((byte) 0x01),
    DocumentDelete((byte) 0x02), DatabaseDelete((byte) 0x03);

    private final byte id;
    TYPE(byte id) { this.id = id; }
    public byte getValue() { return id;}
  }
  
  /**
   * Constructor
   * @param c database context
   * @param a message queue address
   * @param t topic
   */
  public Distributor(final Context c, final String a, final String t)  {
    addr = a;
    context = c;
    topic = t;
  }

  /**
   * Get the address of the message broker.
   * 
   * @return amqp URI
   */
  public String getAddress() {
    return addr;
  }
}
