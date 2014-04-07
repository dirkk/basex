package org.basex.server.replication.tcp;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.OnComplete;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.japi.Procedure;
import akka.util.ByteIterator;
import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import org.basex.core.BaseXException;
import org.basex.core.Command;
import org.basex.core.Context;
import org.basex.core.Replication;
import org.basex.core.cmd.*;
import org.basex.core.parse.CommandParser;
import org.basex.io.in.BufferInput;
import org.basex.io.in.DecodingInput;
import org.basex.io.out.EncodingOutput;
import org.basex.io.out.PrintOutput;
import org.basex.server.AListener;
import org.basex.server.Log;
import org.basex.server.QueryListener;
import org.basex.server.ServerCmd;
import org.basex.util.Performance;
import org.basex.util.Util;
import org.basex.util.list.ByteList;
import scala.concurrent.Future;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Timer;
import java.util.concurrent.Callable;

import static akka.dispatch.Futures.future;
import static org.basex.core.Text.INTERRUPTED;
import static org.basex.core.Text.TIMEOUT_EXCEEDED;
import static org.basex.util.Token.md5;

/**
 * Server-side client session in the replication architecture.
 *
 * @author BaseX Team 2005-14, BSD License
 * @authro Dirk Kirsten
 * @author Andreas Weiler
 * @author Christian Gruen
 */
public final class ClientListenerActor extends UntypedActor implements AListener {
  /** Timer for authentication time out. */
  public final Timer auth = new Timer();
  /** Active queries. */
  private final HashMap<String, QueryListener> queries =
    new HashMap<String, QueryListener>();
  /** Performance measurement. */
  private final Performance perf = new Performance();
  /** Database context. */
  private final Context context;

  /** Write channel. */
  private ActorRef channel;
  /** Replication actor. */
  private ActorRef replication;
  /** Byte string builder for outgoing messages. */
  private final ByteStringBuilder bsb;
  /** Output stream. */
  private PrintOutput out;
  /** Input stream. */
  private BufferInput in;
  /** Current command. */
  private Command command;
  /** Query id counter. */
  private int id;

  /** Authenticated. */
  private boolean authOk = false;
  /** Login timestamp. */
  private String ts;
  /** Timestamp of last interaction. */
  public long last = 0;

  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final Context c, final ActorRef ch, final ActorRef r) {
    return Props.create(ClientListenerActor.class, c, ch, r);
  }

  /**
   * Constructor.
   * @param c database context
   */
  public ClientListenerActor(final Context c, final ActorRef ch, final ActorRef r) {
    context = new Context(c, this);
    channel = ch;
    replication = r;
    bsb = new ByteStringBuilder();
  }

  @Override
  public void preStart() {

  }

  @Override
  public long getId() {
    return 0;
  }

  @Override
  public String address() {
    // TODO
    return null;
  }

  @Override
  public void onReceive(final Object msg) throws Exception {
    // not authenticated
    if (msg instanceof Tcp.Received) {
      ByteString data = ((Tcp.Received) msg).data();
      InputStream is = new ByteIterator.ByteArrayIterator(data.toArray(), 0, data.length()).asInputStream();
      in = new BufferInput(is);
      out = PrintOutput.get(bsb.asOutputStream());

      // receive {PROTOCOL}{USER}{OPTIONS}
      final int protocol = in.read();
      // apply strategy pattern
      switch (protocol) {
        case Replication.PROTOCOL_V1:
          authProtocolV1(in);
          break;
        default:
          sendAuthError(0);
      }
    } else {
      unhandled(msg);
    }
  }

  private void authProtocolV1(final BufferInput bi) throws IOException {
    final String us = in.readString();
    final String[] options = readArray(in);
    System.out.println("Options to server: " + options);

    out.write(0); // OK
    ts = Long.toString(System.nanoTime());
    out.writeString(ts);
    out.write(Replication.HASH_MD5); // at the moment all passwords are md5 encrypted
    out.write(0); // TODO options array

    send();
    getContext().become(new Procedure<Object>() {
      @Override
      public void apply(Object msg) throws Exception {
        if (msg instanceof Tcp.Received) {
          ByteString data = ((Tcp.Received) msg).data();
          InputStream is = new ByteIterator.ByteArrayIterator(data.toArray(), 0, data.length()).asInputStream();
          in = new BufferInput(is);

          final String pw = in.readString();
          context.user = context.users.get(us);
          authOk = context.user != null && md5(context.user.password + ts).equals(pw);

          if (authOk) {
            out.write(0); // authentication successful
            out.write(0); // no options
            send();
            getContext().become(authenticated);
          } else {
            out.write(1); // authentication failed)
            out.write(0); // error code 0 = incorrect credentials
            out.write(0); // no options
          }
        } else {
          unhandled(msg);
        }
      }
    });
  }

  private void sendAuthError(final int errorCode) throws IOException {
    out.write(1);
    out.write(errorCode);
    out.write(0);
  }

  private String[] readArray(final BufferInput in) throws IOException {
    final int l = in.read();
    final String[] arr = new String[l];
    for (int i = 0; i < l; ++i) arr[i] = in.readString();
    return arr;
  }

  private Procedure<Object> authenticated = new Procedure<Object>() {
    /**
     * Parse and process incoming data from a socket after successful authentication.
     * @param msg
     */
    @Override
    public void apply(Object msg) throws Exception {
      if (msg instanceof Tcp.Received) {
        ByteString data = ((Tcp.Received) msg).data();
        in = new BufferInput(new ByteIterator.ByteArrayIterator(data.toArray(), 0, data.length()).asInputStream());
        out = PrintOutput.get(bsb.asOutputStream());

        command = null;
        String cmd;
        final ServerCmd sc;
        try {
          final int b = in.read();

          last = System.currentTimeMillis();
          perf.time();
          sc = ServerCmd.get(b);
          cmd = null;
          if(sc == ServerCmd.CREATE) {
            create();
          } else if(sc == ServerCmd.ADD) {
            add();
          } else if(sc == ServerCmd.REPLACE) {
            replace();
          } else if(sc == ServerCmd.STORE) {
            store();
          } else if(sc != ServerCmd.COMMAND) {
            query(sc);
          } else {
            // database command
            cmd = new ByteList().add(b).add(in.readBytes()).toString();
          }
        } catch(final IOException ex) {
          // this exception may be thrown if a session is stopped
          quit();
          return;
        }
        if(sc != ServerCmd.COMMAND) return;

        // parse input and create command instance in a future as it could be blocking
        final String finalCmd = cmd;
        Future<Command> f = future(new Callable<Command>() {
          @Override
          public Command call() throws Exception {
            return new CommandParser(finalCmd, context).parseSingle();
          }
        }, context().dispatcher());
        f.onFailure(new OnFailure() {
          @Override
          public void onFailure(Throwable ex) throws Throwable {
            // log invalid command
            final String emsg = ex.getMessage();
            log(finalCmd, null);
            log(emsg, false);
            // send 0 to mark end of potential result
            out.write(0);
            // send {INFO}0
            out.writeString(emsg);
            // send 1 to mark error
            send(false);
          }
        }, context().dispatcher());
        f.onSuccess(new OnSuccess<Command>() {
          @Override
          public void onSuccess(Command c) throws Throwable {
            command = c;
            log(command, null);

            // execute command and send {RESULT}
            Future<Object> f = future(new Callable<Object>() {
              @Override
              public Object call() throws Exception {
                // run command
                command.execute(context, new EncodingOutput(out));
                return null;
              }
            }, context().dispatcher());
            f.onComplete(new OnComplete<Object>() {
              @Override
              public void onComplete(Throwable ex, Object o) throws Throwable {
                boolean ok = true;
                String info = command.info();

                if (ex != null) {
                  if (ex instanceof BaseXException) {
                    ok = false;
                    info = ex.getMessage();
                    if(info.startsWith(INTERRUPTED)) info = TIMEOUT_EXCEEDED;
                  } else if (ex instanceof IOException) {
                    log(ex, false);
                    command = null;
                    quit();
                  } else {
                    log(ex, false);
                  }
                }

                // send 0 to mark end of result
                out.write(0);
                // send info
                info(info, ok);

                // stop console
                if(command instanceof Exit) {
                  command = null;
                  quit();
                }
              }
            }, context().dispatcher());
          }
        }, context().dispatcher());

        command = null;
      } else {
        unhandled(msg);
      }
    }
  };

  /**
   * Checks, whether this connection is timed out. If it is inactive, the connection
   * will be dropped.
   *
   * @param timeout allowed timeout
   * @return is inactive
   */
  public boolean isInactive(final long timeout) {
    final long ms = System.currentTimeMillis();
    final boolean inactive = ms - last > timeout;
    if(inactive) quit();

    return inactive;
  }

  /**
   * Quits the authentication.
   */
  public synchronized void quitAuth() {
    try {
      // TODO terminate this actor
      log(TIMEOUT_EXCEEDED, false);
    } catch(final Throwable ex) {
      log(ex, false);
    }
  }

  /**
   * Exits the session.
   */
  @Override
  public synchronized void quit() {
    // wait until running command was stopped
    if(command != null) {
      command.stop();
      do Performance.sleep(50); while(command != null);
    }
//    context.sessions.remove(this);

    try {
      if (context != null) new Close().run(context);
      // TODO terminate this actor
    } catch(final Throwable ex) {
      log(ex, false);
      Util.stack(ex);
    }
  }

  @Override
  public Context dbCtx() {
    return context;
  }

  @Override
  public long last() {
    return 0;
  }

  @Override
  public void notify(byte[] name, byte[] msg) throws IOException {

  }

  // PRIVATE METHODS ==========================================================

  /**
   * Creates a database.
   * @throws java.io.IOException I/O exception
   */
  private void create() throws IOException {
    execute(new CreateDB(in.readString()));
  }

  /**
   * Adds a document to a database.
   * @throws java.io.IOException I/O exception
   */
  private void add() throws IOException {
    execute(new Add(in.readString()));
  }

  /**
   * Replace a document in a database.
   * @throws java.io.IOException I/O exception
   */
  private void replace() throws IOException {
    execute(new Replace(in.readString()));
  }

  /**
   * Stores raw data in a database.
   * @throws java.io.IOException I/O exception
   */
  private void store() throws IOException {
    execute(new Store(in.readString()));
  }

  /**
   * Executes the specified command.
   * @param cmd command to be executed
   * @throws java.io.IOException I/O exception
   */
  private void execute(final Command cmd) throws IOException {
    log(cmd + " [...]", null);
    final DecodingInput di = new DecodingInput(in);
    cmd.setInput(di);

    // run the database execution in a future as it is a blocking operation
    scala.concurrent.Future<String> f = future(new Callable<String>() {
      @Override
      public String call() throws Exception {
        cmd.execute(context);
        return cmd.info();
      }
    }, context().dispatcher());

    f.onComplete(new OnComplete<String>() {
      @Override
      public void onComplete(Throwable throwable, String s) throws Throwable {
        if (throwable != null) {
          di.flush();
          error(throwable.getMessage());
        } else {
          success(s);
        }
      }
    }, context().dispatcher());
  }

  /**
   * Processes the query iterator.
   * @param sc server command
   * @throws java.io.IOException I/O exception
   */
  private void query(final ServerCmd sc) throws IOException {
    // iterator argument (query or identifier)
    String arg = in.readString();

    final QueryListener qp;
    final StringBuilder info = new StringBuilder();
    if(sc == ServerCmd.QUERY) {
      final String query = arg;
      qp = new QueryListener(query, context);
      arg = Integer.toString(id++);
      queries.put(arg, qp);
      // send {ID}0
      out.writeString(arg);
      // write log file
      info.append(query);
      // send 0 as success flag
      out.write(0);
      // write log file
      log(new StringBuilder(sc.toString()).append('[').
        append(arg).append("] ").append(info), true);
      out.flush();
    } else {
      // find query process
      qp = queries.get(arg);
      // ID has already been removed
      if(qp == null) {
        if(sc != ServerCmd.CLOSE) throw new IOException("Unknown Query ID: " + arg);
      } else if(sc == ServerCmd.BIND) {
        final String key = in.readString();
        final String val = in.readString();
        final String typ = in.readString();
        qp.bind(key, val, typ);
        info.append(key).append('=').append(val);
        if(!typ.isEmpty()) info.append(" as ").append(typ);

        // send 0 as end marker
        out.write(0);
        // send 0 as success flag
        out.write(0);
        // write log file
        log(new StringBuilder(sc.toString()).append('[').
          append(arg).append("] ").append(info), true);
        out.flush();
      } else if(sc == ServerCmd.CONTEXT) {
        final String val = in.readString();
        final String typ = in.readString();
        qp.context(val, typ);
        info.append(val);
        if(!typ.isEmpty()) info.append(" as ").append(typ);

        // send 0 as end marker
        out.write(0);
        // send 0 as success flag
        out.write(0);
        // write log file
        log(new StringBuilder(sc.toString()).append('[').
          append(arg).append("] ").append(info), true);
        out.flush();
      } else if(sc == ServerCmd.RESULTS) {
        qp.execute(true, out, true, false);
      } else if(sc == ServerCmd.EXEC) {
        Future<Object> f = future(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            qp.execute(false, out, true, false);
            return null;
          }
        }, context().dispatcher());
        final String finalArg = arg;
        f.onFailure(new OnFailure() {
          @Override
          public void onFailure(Throwable ex) throws Throwable {
            // log exception (static or runtime)
            final String err = Util.message(ex);
            log(sc + "[" + finalArg + ']', null);
            log(err, false);
            queries.remove(finalArg);

            // send 0 as end marker, 1 as error flag, and {MSG}0
            out.write(0);
            out.write(1);
            out.writeString(err);
          }
        }, context().dispatcher());
      } else if(sc == ServerCmd.FULL) {
        qp.execute(true, out, true, true);
      } else if(sc == ServerCmd.INFO) {
        out.print(qp.info());
        // send 0 as end marker
        out.write(0);
        // send 0 as success flag
        out.write(0);
        // write log file
        log(new StringBuilder(sc.toString()).append('[').
          append(arg).append("] ").append(info), true);
        out.flush();
      } else if(sc == ServerCmd.OPTIONS) {
        out.print(qp.parameters());
        // send 0 as end marker
        out.write(0);
        // send 0 as success flag
        out.write(0);
        // write log file
        log(new StringBuilder(sc.toString()).append('[').
          append(arg).append("] ").append(info), true);
        out.flush();
      } else if(sc == ServerCmd.UPDATING) {
        out.print(Boolean.toString(qp.updating()));
        // send 0 as end marker
        out.write(0);
        // send 0 as success flag
        out.write(0);
        // write log file
        log(new StringBuilder(sc.toString()).append('[').
          append(arg).append("] ").append(info), true);
        out.flush();
      } else if(sc == ServerCmd.CLOSE) {
        queries.remove(arg);
        // send 0 as end marker
        out.write(0);
        // send 0 as success flag
        out.write(0);
        // write log file
        log(new StringBuilder(sc.toString()).append('[').
          append(arg).append("] ").append(info), true);
        out.flush();
      } else if(sc == ServerCmd.NEXT) {
        // log exception (static or runtime)
        final String err = "Protocol for query iteration is out-of-date.";
        log(sc + "[" + arg + ']', null);
        log(err, false);
        queries.remove(arg);

        // send 0 as end marker, 1 as error flag, and {MSG}0
        out.write(0);
        out.write(1);
        out.writeString(err);
      }
    }
  }

  /**
   * Writes a log message.
   * @param info message info
   * @param type message type (true/false/null: OK, ERROR, REQUEST)
   */
  protected void log(final Object info, final Object type) {
    // add evaluation time if any type is specified
    final String user = context.user != null ? context.user.name : "";
    final Log log = context.log;
    if(log != null) log.write(type != null ?
            new Object[] { address(), user, type, info, perf } :
            new Object[] { address(), user, null, info });
  }

  /**
   * Returns user feedback.
   * @param info information string
   * @param ok success/error flag
   * @throws IOException I/O exception
   */
  protected void info(final String info, final boolean ok) throws IOException {
    // write feedback to log file
    log(info, ok);
    // send {MSG}0 and (0|1) as (success|error) flag
    out.writeString(info);
    send(ok);
  }

  /**
   * Returns error feedback.
   * @param info error string
   * @throws IOException I/O exception
   */
  protected void error(final String info) throws IOException {
    info(info, false);
  }

  /**
   * Returns user feedback.
   * @param info information string
   * @throws IOException I/O exception
   */
  protected void success(final String info) throws IOException {
    info(info, true);
  }

  /**
   * Sends a success flag to the client (0: true, 1: false).
   * @param ok success flag
   * @throws IOException I/O exception
   */
  protected void send(final boolean ok) throws IOException {
    out.write(ok ? 0 : 1);
    send();
  }

  protected void send() throws IOException {
    out.flush();

    channel.tell(TcpMessage.write(bsb.result()), getSelf());
    bsb.clear();
  }

  protected Tcp.Command getOutput() throws IOException {
    out.flush();

    Tcp.Command cmd = TcpMessage.write(bsb.result());
    bsb.clear();
    return cmd;
  }
}
