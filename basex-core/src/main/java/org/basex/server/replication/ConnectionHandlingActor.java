package org.basex.server.replication;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import org.basex.core.Context;

import java.util.Iterator;

import static org.basex.server.replication.ConnectionMessages.ConnectionStart;
import static org.basex.server.replication.ConnectionMessages.SyncFinished;

/**
 * This actor handles a new incoming member on a primary within a replica set. It must
 * be created as child to a {@link org.basex.server.replication.ReplicationActor}.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ConnectionHandlingActor extends UntypedActor {
  /** Replica set. */
  private final ReplicaSet set;
  /** Database context. */
  private final Context dbContext;
  /** Timeout. */
  private final Timeout timeout;
  /** Logging. */
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);


  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final ReplicaSet set, final Context dbContext, final Timeout timeout) {
    return Props.create(ConnectionHandlingActor.class, set, dbContext, timeout);
  }

  /**
   * Constructor.
   *
   * @param dbContext database context
   * @param timeout standard timeout
   */
  public ConnectionHandlingActor(final ReplicaSet set, final Context dbContext, final Timeout timeout) {
    this.set = set;
    this.dbContext = dbContext;
    this.timeout = timeout;
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof ConnectionStart) {
      log.info("Got a connection start from {}", getSender().path());
      ConnectionStart resp = (ConnectionStart) msg;
      org.basex.server.replication.Member m = new org.basex.server.replication.Member(getSender(), resp.getId(), resp.isVoting());

      for (Iterator<String> it = dbContext.databases.listDBs().iterator(); it.hasNext();) {
        String db = it.next();

        // TODO respect timestamps
        if (!resp.getDatabases().containsKey(db)) {
          // todo send actual database content
          byte [] d = {0x00};
          log.info("Sync database {} to {}", db, getSender().path());
          getSender().tell(new ConnectionMessages.SyncDatabase(db, d), getSelf());
        }
      }

      log.info("Connection to member {} established, ID: {}", getSender().path(), resp.getId());
      // send connection finished message
      getContext().system().actorSelection(resp.getId() + "/user/replication").tell(
        new SyncFinished(set.getState(), set.getPrimary(), set.getSecondaries()),
        getSelf()
      );
      getContext().parent().tell(m, getSelf());

      // terminate yourself
      getContext().stop(getSelf());
    } else {
      unhandled(msg);
    }
  }
}
