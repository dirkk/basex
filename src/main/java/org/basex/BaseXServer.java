package org.basex;


import static org.basex.core.Text.*;

import java.io.*;
import java.net.*;

import org.basex.core.*;
import org.basex.server.*;
import org.basex.server.client.*;
import org.basex.util.*;
import org.basex.util.list.*;

import scala.concurrent.*;
import scala.concurrent.duration.*;

import com.typesafe.config.*;

import akka.actor.*;
import akka.pattern.*;
import akka.util.*;

/**
 * This is the mostly unblocking server version of BaseX. It uses akka
 * and an event-based approach to handle connections and should be able to
 * serve a high number of concurrent connections.
 * 
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 * @author Andreas Weiler
 * @author Dirk Kirsten
 */
public final class BaseXServer extends Main {
  /** Actor system. This is a heavy-weight structure. */
  private ActorSystem system;
  /** Initial commands. */
  private StringList commands;
  /** Start as daemon. */
  private boolean service;
  /** Use replication (master/slave infrastructure). */
  private boolean replication = true;

  /**
   * Main method, launching the server process.
   * Command-line arguments are listed with the {@code -h} argument.
   * @param args command-line arguments
   */
  public static void main(final String[] args) {
    try {
      new BaseXServer(args);
    } catch(final Exception ex) {
      Util.errln(ex);
      System.exit(1);
    }
  }

  /**
   * Constructor.
   * @param args command-line arguments
   * @throws IOException I/O exception
   */
  public BaseXServer(final String... args) throws IOException {
    this(null, args);
  }

  /**
   * Constructor.
   * @param ctx database context
   * @param args command-line arguments
   * @throws IOException I/O exception
   */
  public BaseXServer(final Context ctx, final String... args) throws IOException {
    super(args, ctx);
    
    final MainProp mprop = context.mprop;
    
    // parse and set all config options from the BaseX settings
    int port = mprop.num(MainProp.SERVERPORT);
    String host = mprop.get(MainProp.SERVERHOST);
    
    // parse the Akka configuration and merge both configs, letting BaseX
    // specific config win
    Config regularConfig = ConfigFactory.load().getConfig("server");

    InetSocketAddress master = null;
    int mport = mprop.num(MainProp.MASTERPORT);
    String mhost = mprop.get(MainProp.MASTERHOST);
    if (replication && mhost != null && mhost.length() > 0 && mport != 0) {
      master = new InetSocketAddress(mhost, mport);
    }

    if(service) {
      Performance.sleep(1000);
      return;
    }

    try {
      // execute command-line arguments
      for(final String c : commands) execute(c);

      // set up actor system
      system = ActorSystem.create("BaseXServer", regularConfig);
      ActorRef server = system.actorOf(ServerActor.mkProps(
          new InetSocketAddress(host, port), 
          new InetSocketAddress(host, mprop.num(MainProp.EVENTPORT)),
          master,
          context), "server");
      
      // wait for socket to be bound
      try {
        Timeout timeout = new Timeout(Duration.create(5, "seconds"));
        Future<Object> future = Patterns.ask(server, "bound", timeout);
        Await.result(future, timeout.duration());
      } catch(final Exception ex) {
        throw new BaseXException("Could not bind to socket");
      }
      
      if(console) {
        console();
        quit();
      }
    } catch(final Exception ex) {
      context.log.writeError(ex);
      throw new IOException(ex);
    }
  }

  @Override
  protected synchronized void quit() throws IOException {
    
  }

  @Override
  protected Session session() {
    if(session == null) session = new LocalSession(context, out);
    return session;
  }

  @Override
  protected void parseArguments(final String... args) throws IOException {
    final Args arg = new Args(args, this, SERVERINFO, Util.info(CONSOLE, SERVERMODE));
    commands = new StringList();

    while(arg.more()) {
      if(arg.dash()) {
        switch(arg.next()) {
          case 'c': // send database commands
            commands.add(arg.string());
            break;
          case 'd': // activate debug mode
            Prop.debug = true;
            break;
          case 'e': // parse event port
            context.mprop.set(MainProp.EVENTPORT, arg.number());
            break;
          case 'i': // activate interactive mode
            console = true;
            break;
          case 'h': // parse host + port the server is bound to
            String[] all = arg.string().split(":");
            context.mprop.set(MainProp.SERVERHOST, all[0]);
            if (all.length >= 2)
              context.mprop.set(MainProp.SERVERPORT, Integer.valueOf(all[1]));
            break;
          case 'm': // parse host + port of the master server to subscribe to
            all = arg.string().split(":");
            context.mprop.set(MainProp.MASTERHOST, all[0]);
            if (all.length >= 2)
              context.mprop.set(MainProp.MASTERPORT, Integer.valueOf(all[1]));
            break;
          case 'S': // set service flag
            service = true;
            break;
          case 'u': // do no use replication
            replication = false;
            break;
          case 'z': // suppress logging
            context.mprop.set(MainProp.LOG, false);
            break;
          default:
            arg.usage();
        }
      } else {
        arg.usage();
      }
    }
  }
  
  /**
   * Stops the server of this instance.
   */
  public void stop() {
    system.shutdown();
    system.awaitTermination();
  }
}
