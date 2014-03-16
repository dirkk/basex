package org.basex;

import org.basex.core.BaseXException;
import org.basex.core.Context;
import org.basex.core.GlobalOptions;
import org.basex.core.Main;
import org.basex.io.IOFile;
import org.basex.server.ClientSession;
import org.basex.server.LocalSession;
import org.basex.server.LoginException;
import org.basex.server.Session;
import org.basex.util.Args;
import org.basex.util.Performance;
import org.basex.util.Prop;
import org.basex.util.Util;
import org.basex.util.list.StringList;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.basex.core.Text.*;

/**
 * This is the starter class for running the database server. It handles
 * concurrent requests from multiple users.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 * @author Andreas Weiler
 * @author Dirk Kirsten
 */
public final class BaseXMember extends Main implements Runnable {
  /** Flag for server activity. */
  private volatile boolean running;
  /** Stop file. */
  private IOFile stop;

  /** Stopped flag. */
  private volatile boolean stopped;
  /** Initial commands. */
  private StringList commands;
  /** Server socket. */
  private ServerSocket socket;
  /** Start as daemon. */
  private boolean service;

  /** Host used for binding the TCP socket for clients and the akka system to. */
  private String host;
  /** Port the local akka system is binding to. */
  private int akkaPort;
  /** Socket address for clients to connect to. */
  private InetSocketAddress tcpAddr;
  /** Address for another BaseXMember to connect to. */
  private InetSocketAddress connectTo;

  /**
   * Main method, launching the server process.
   * Command-line arguments are listed with the {@code -h} argument.
   * @param args command-line arguments
   */
  public static void main(final String[] args) {
    try {
      new BaseXMember(args);
    } catch(final IOException ex) {
      Util.errln(ex);
      System.exit(1);
    }
  }

  /**
   * Constructor.
   * @param args command-line arguments
   * @throws java.io.IOException I/O exception
   */
  public BaseXMember(final String... args) throws IOException {
    this(null, args);
  }

  /**
   * Constructor.
   * @param ctx database context
   * @param args command-line arguments
   * @throws java.io.IOException I/O exception
   */
  public BaseXMember(final Context ctx, final String... args) throws IOException {
    super(args, ctx);
    final GlobalOptions gopts = context.globalopts;
    final int port = gopts.get(GlobalOptions.SERVERPORT);
    akkaPort = gopts.get(GlobalOptions.AKKAPORT);

    host = gopts.get(GlobalOptions.SERVERHOST);
    tcpAddr = host.isEmpty() ? null : new InetSocketAddress(host, port);

    if(service) {
      start(port, args);
      Util.outln(SRV_STARTED_PORT_X, port);
      Performance.sleep(1000);
      return;
    }

    if(stopped) {
      stop(port);
      Util.outln(SRV_STOPPED_PORT_X, port);
      Performance.sleep(1000);
      return;
    }

    try {
      // execute command-line arguments
      for(final String c : commands) execute(c);

      stop = stopFile(port);

      // show info when server is aborted
      context.log.writeServer(OK, Util.info(SRV_STARTED_PORT_X, port));
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          context.log.writeServer(OK, Util.info(SRV_STOPPED_PORT_X, port));
          Util.outln(SRV_STOPPED_PORT_X, port);
        }
      });

      new Thread(this).start();
      while(!running) Performance.sleep(10);

      Util.outln(S_CONSOLE + (console ? TRY_MORE_X :
        Util.info(SRV_STARTED_PORT_X, port)), S_SERVER);

      if(console) {
        console();
        quit();
      }
    } catch(final IOException ex) {
      context.log.writeError(ex);
      throw ex;
    }
  }

  @Override
  public void run() {
    context.replication.start(context, new InetSocketAddress(host, akkaPort), tcpAddr);

    if (connectTo != null)
      context.replication.connect(connectTo);

    running = true;
  }

  /**
   * Generates a stop file for the specified port.
   * @param port server port
   * @return stop file
   */
  private static IOFile stopFile(final int port) {
    return new IOFile(Prop.TMP, Util.className(BaseXMember.class) + port);
  }

  @Override
  protected synchronized void quit() throws IOException {
    if(!running) return;
    running = false;

    try {
      // close interactive input if server was stopped by another process
      if(console) System.in.close();
      socket.close();
    } catch(final IOException ex) {
      Util.errln(ex);
      context.log.writeError(ex);
    }
    console = false;
  }

  @Override
  protected Session session() {
    if(session == null) session = new LocalSession(context, out);
    return session;
  }

  @Override
  protected void parseArguments(final String... args) throws IOException {
    final Args arg = new Args(args, this, S_SERVERINFO, Util.info(S_CONSOLE, S_SERVER));
    commands = new StringList();
    boolean daemon = false;

    while(arg.more()) {
      if(arg.dash()) {
        switch(arg.next()) {
          case 'a': // local akka port address
            context.globalopts.set(GlobalOptions.AKKAPORT, arg.number());
            break;
          case 'c': // send database commands
            commands.add(arg.string());
            break;
          case 'd': // activate debug mode
            Prop.debug = true;
            break;
          case 'D': // hidden flag: daemon mode
            daemon = true;
            break;
          case 'i': // activate interactive mode
            console = true;
            break;
          case 'n': // parse host the server is bound to
            context.globalopts.set(GlobalOptions.SERVERHOST, arg.string());
            break;
          case 'p': // parse server port
            context.globalopts.set(GlobalOptions.SERVERPORT, arg.number());
            break;
          case 'S': // set service flag
            service = !daemon;
            break;
          case 'x': // connect to this akka system, given in the format host:port
            String[] in = arg.string().split(":");
            connectTo = new InetSocketAddress(in[0], Integer.valueOf(in[1]));
            break;
          case 'z': // suppress logging
            context.globalopts.set(GlobalOptions.LOG, false);
            break;
          default:
            throw arg.usage();
        }
      } else {
        if("stop".equalsIgnoreCase(arg.string())) {
          stopped = true;
        } else {
          throw arg.usage();
        }
      }
    }
  }

  /**
   * Stops the server of this instance.
   * @throws java.io.IOException I/O exception
   */
  public void stop() throws IOException {
    final GlobalOptions gopts = context.globalopts;
    stop(gopts.get(GlobalOptions.SERVERPORT));
    context.replication.stop();
  }

  // STATIC METHODS ===========================================================

  /**
   * Starts the database server in a separate process.
   * @param port server port
   * @param args command-line arguments
   * @throws org.basex.core.BaseXException database exception
   */
  public static void start(final int port, final String... args) throws BaseXException {
    // check if server is already running (needs some time)
    if(ping(S_LOCALHOST, port)) throw new BaseXException(SRV_RUNNING);

    Util.start(BaseXMember.class, args);

    // try to connect to the new server instance
    for(int c = 1; c < 10; ++c) {
      if(ping(S_LOCALHOST, port)) return;
      Performance.sleep(c * 100);
    }
    throw new BaseXException(CONNECTION_ERROR);
  }

  /**
   * Checks if a server is running.
   * @param host host
   * @param port server port
   * @return boolean success
   */
  public static boolean ping(final String host, final int port) {
    try {
      // connect server with invalid login data
      new ClientSession(host, port, "", "");
      return false;
    } catch(final LoginException ex) {
      // if login was checked, server is running
      return true;
    } catch(final IOException ex) {
      return false;
    }
  }

  /**
   * Stops the server.
   * @param port server port
   * @throws java.io.IOException I/O exception
   */
  public static void stop(final int port) throws IOException {
    final IOFile stop = stopFile(port);

    stop.touch();
    // wait and check if server was really stopped
    do Performance.sleep(50); while(ping(S_LOCALHOST, port));

    stop.delete();
  }
}
