package org.basex.server;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.basex.core.*;
import org.basex.core.cmd.*;
import org.basex.core.parse.*;
import org.basex.io.in.*;
import org.basex.query.*;
import org.basex.server.messages.*;
import org.basex.util.*;

import scala.concurrent.duration.*;

import akka.actor.*;
import akka.event.*;
import akka.io.Tcp.ConnectionClosed;
import akka.io.Tcp.Received;
import akka.japi.*;
import akka.routing.*;
import akka.util.*;

import static org.basex.util.Token.md5;
import static org.basex.core.Text.*;

/**
 * Actor class to handle a connected client at the server-side. This class
 * is non-blocking.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ClientHandler extends UntypedActor {
  /** Logging adapter. */
  protected final LoggingAdapter log;
  /** Authentification timestamp. */
  protected String ts;
  /** Database context. */
  protected Context dbContext;
  /** Query id counter. */
  protected Integer id = 0;
  /** Active queries. */
  protected final HashMap<Integer, ActorRef> queries =
    new HashMap<Integer, ActorRef>();
  /** Connected client address. */
  protected Address addr;
  /** Event Socket address already sent? */
  protected boolean addressSend = false;
  /** Waiting for a WATCH command. Stateful protocol. */
  protected boolean watchCommand = false;
  /** Event handling actor. */
  protected ActorRef event;
  /** Worker pool to do blocking command execution. */
  protected ActorRef cmdWorker;
  /** Current command. */
  private Command command;
  
  /**
   * Create Props for the client handler actor.
   * @param ctx database context
   * @return Props for creating this actor, can be further configured
   */
  public static Props mkProps(final Context ctx) {
    return Props.create(ClientHandler.class, ctx);
  }
  
  /**
   * Constructor.
   * @param ctx database context
   */
  public ClientHandler(final Context ctx) {
    log = Logging.getLogger(getContext().system(), this);
    dbContext = new Context(ctx, null);
    cmdWorker = getContext().actorOf(CommandActor.mkProps(dbContext)
        .withRouter(new RoundRobinRouter(10)),
        "CmdExec");
  }
  
  @Override
  public void onReceive(Object msg) throws Exception {
    // client is not authenticated
    if (msg instanceof String && ((String) msg).equalsIgnoreCase("connect")) {
      ts = Long.toString(System.nanoTime());
      new Writer()
        .writeString(ts)
        .send(getSender(), getSelf());
    } else if (msg instanceof Received) {
      // client sends username + hashed passsword
      ByteString data = ((Received) msg).data();
      Reader r = new Reader(data);
      final String user = r.getString();
      final String hashedPw = r.getString();
      
      dbContext.user = dbContext.users.get(user);
      if (md5(dbContext.user.password + ts).equals(hashedPw)) {
        log.info("Authentification successful for user {}", dbContext.user);
        addr = getSender().path().address();
        
        // send successful response message
        new Writer().writeTerminator().send(getSender(), getSelf());
        
        // change incoming message processing
        getContext().become(authenticated());
      } else {
        log.info("Access denied for user {}", dbContext.user);
        
        // send authentication denied message with 1s delay
        new Writer()
          .writeSuccess(false)
          .sendDelayed(getSender(), getSelf(), getContext().system(), Duration.create(1, TimeUnit.SECONDS));
      }
    } else if (msg instanceof ConnectionClosed) {
      quit();
    } else {
      unhandled(msg);
    }
  }
  
  /**
   * Message handling procedure when already authenticated, i.e. normal
   * operational mode.
   * @return normal authenticated procedure
   */
  private Procedure<Object> authenticated() {
    return new Procedure<Object>() {
      @Override
      public void apply(Object msg) throws Exception {
        if (msg instanceof Received) {
          Received recv = (Received) msg;
          Reader reader = new Reader(recv.data());
          
          if (watchCommand) {
            watch(reader.getString());
            watchCommand = false;
          } else {
            // get the command byte
            ServerCmd sc = ServerCmd.get(reader.getByte());
            if (sc == ServerCmd.CREATE) {
              create(reader.getString(), reader.getInputStream());
            } else if (sc == ServerCmd.ADD) {
              add(reader.getString(), reader.getInputStream());
            } else if (sc == ServerCmd.REPLACE) {
              replace(reader.getString(), reader.getInputStream());
            } else if (sc == ServerCmd.STORE) {
              store(reader.getString(), reader.getInputStream());
            } else if (sc == ServerCmd.WATCH) {
              watch(reader.getString());
            } else if (sc == ServerCmd.UNWATCH) {
              unwatch(reader.getString());
            } else if (sc == ServerCmd.QUERY) {
              newQuery(msg);
            } else if (sc == ServerCmd.CLOSE) {
              int queryId = Integer.decode(reader.getString());
              if (queries.containsKey(queryId)) {
                ActorRef query = queries.get(queryId);
                query.forward(msg, getContext());
                queries.remove(queryId);
              } else {
                new Writer().writeTerminator().writeTerminator()
                  .send(getSender(), getSelf());
              }
            } else if (sc == ServerCmd.BIND || sc == ServerCmd.CONTEXT ||
                sc == ServerCmd.ITER || sc == ServerCmd.EXEC || sc == ServerCmd.FULL ||
                sc == ServerCmd.INFO || sc == ServerCmd.OPTIONS || sc == ServerCmd.UPDATING) {
              int queryId = Integer.decode(reader.getString());
              if (queries.containsKey(queryId)) {
                ActorRef query = queries.get(queryId);
                query.forward(msg, getContext());
              } else {
                throw new IOException("Unknown Query ID: " + queryId);
              }
            } else if (sc == ServerCmd.COMMAND) {
              reader.decreaseReader();
              command(reader);
            }
          }
        } else if (msg instanceof ConnectionClosed) {
          quit();
        } else if (msg instanceof ActorRef) {
          register((ActorRef) msg);
        } else {
          unhandled(msg);
        }
      }
    };
  }
  
  /**
   * Returns the database context of this session.
   * @return user reference
   */
  public Context dbContext() {
    return dbContext;
  }
  

  /**
   * Exits the session.
   */
  public synchronized void quit() {
    // wait until running command was stopped
    if(command != null) {
      command.stop();
    }
    dbContext().sessions.remove(this);
    
    try {
      new Close().run(dbContext());
      // remove this session from all events in pool
      for(final Sessions s : dbContext().events.values()) s.remove(this);
    } catch(final Throwable ex) {
      log.error(ex.getMessage());
    }

    getContext().stop(getSelf());
  }
  
  /**
   * Creates a database.
   * @param name database name
   * @param input input stream
   */
  protected void create(final String name, final InputStream input) {
    execute(new CreateDB(name), input);
  }

  /**
   * Adds a document to a database.
   * @param path document path
   * @param input input stream
   */
  protected void add(final String path, final InputStream input) {
    execute(new Add(path), input);
  }

  /**
   * Replace a document in a database.
   * @param path document path
   * @param input input stream
   */
  protected void replace(final String path, final InputStream input) {
    execute(new Replace(path), input);
  }

  /**
   * Stores raw data in a database.
   * @param path document path
   * @param input input stream
   */
  protected void store(final String path, final InputStream input) {
    execute(new Store(path), input);
  }
  
  /**
   * Sends a notification to the client.
   * @param name event name
   * @param msg event message
   */
  public synchronized void notify(final byte[] name, final byte[] msg) {
    new Writer().writeBytes(name).writeBytes(msg)
      .send(event, getSelf());
  }

  /**
   * Watches an event.
   * @param name event name
   */
  protected void watch(final String name) {
    if(!addressSend) {
      new Writer()
        .writeString(Integer.toString(dbContext.mprop.num(MainProp.EVENTPORT)))
        .writeString(getSelf().path().name())
        .send(getSender(), getSelf());
      addressSend = true;
      watchCommand = true;
    } else {
      final Sessions s = dbContext.events.get(name);
      final boolean ok = s != null && !s.contains(this);
      final String message;
      if(ok) {
        s.add(this);
        message = WATCHING_EVENT_X;
      } else if(s == null) {
        message = EVENT_UNKNOWN_X;
      } else {
        message = EVENT_WATCHED_X;
      }
  
      new Writer().writeString(message).writeSuccess(ok).send(getSender(), getSelf());
    }
  }

  /**
   * Unwatches an event.
   * @param name event name
   */
  protected void unwatch(final String name) {
    final Sessions s = dbContext.events.get(name);
    final boolean ok = s != null && s.contains(this);
    final String message;
    if(ok) {
      s.remove(this);
      message = UNWATCHING_EVENT_X;
    } else if(s == null) {
      message = EVENT_UNKNOWN_X;
    } else {
      message = EVENT_NOT_WATCHED_X;
    }

    new Writer().writeString(message + name).writeSuccess(ok).send(getSender(), getSelf());
  }
  
  /**
   * Creates a new query.
   * @param msg message
   */
  protected void newQuery(final Object msg) {
    int newId;
    synchronized (id) {
      newId = id++;
    }
    ActorRef query = getContext().actorOf(QueryHandler.mkProps(dbContext, newId));
    queries.put(newId, query);
    query.forward(msg, getContext());
  }

  /**
   * Executes the specified command.
   * @param cmd command to be executed
   * @param input encoded input stream
   */
  protected void execute(final Command cmd, final InputStream input) {
    final DecodingInput di = new DecodingInput(input);
    cmd.setInput(di);
    cmdWorker.tell(new CommandMessage(cmd), getSender());
  }

  /**
   * Parse the command and pass the command execution on to the
   * {@link ClientHandler#cmdWorker} worker pool for blocking command
   * execution.
   *
   * @param reader incoming message reader
   */
  protected void command(final Reader reader) {
    String cmd = reader.getString();
    
    // parse input and create command instance
    try {
      command = new CommandParser(cmd, dbContext).parseSingle();
      log.info(command.toString());
    } catch(final QueryException ex) {
      // log invalid command
      final String msg = ex.getMessage();
      log.info("Query failed: {}, Error message: {}", cmd, msg);
      
      new Writer().writeTerminator().writeString(msg).writeSuccess(false)
        .send(getSender(), getSelf());
      return;
    }

    cmdWorker.tell(new CommandMessage(command, true), getSender());
  }
  
  /**
   * Registers an event actor
   * @param eventActor register this actor
   */
  protected void register(final ActorRef eventActor) {
    event = eventActor;
    log.info("Event registered");
    
    new Writer().writeTerminator().send(event, getSelf());
  }

  /**
   * Returns the host and port of a client.
   * @return string representation
   */
  public String address() {
    return addr.toString();
  }
}
