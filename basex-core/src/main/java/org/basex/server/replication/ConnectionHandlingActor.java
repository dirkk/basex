package org.basex.server.replication;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.Timeout;
import org.basex.core.Context;
import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.io.out.ArrayOutput;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import static org.basex.data.DataText.*;
import static org.basex.server.replication.ConnectionMessages.ConnectionStart;
import static org.basex.server.replication.ConnectionMessages.SyncFinished;
import static org.basex.server.replication.DataMessages.DatabaseMessage;

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
  private final Context dbCtx;
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
    this.dbCtx = dbContext;
    this.timeout = timeout;
  }

  private byte[] getData(final IOFile src) {
    // optimize buffer size
    final int bsize = (int) Math.max(1, Math.min(src.length(), 1 << 22));
    final byte[] buf = new byte[bsize];

    final ArrayOutput ao = new ArrayOutput();
    InputStream fis = null;
    try {
      fis = src.inputStream();
      // copy file buffer by buffer
      for(int i; (i = fis.read(buf)) != -1;) ao.write(buf, 0, i);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ao.toArray();
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof ConnectionStart) {
      log.info("Got a connection start from {}", getSender().path());
      ConnectionStart resp = (ConnectionStart) msg;
      org.basex.server.replication.Member m = new org.basex.server.replication.Member(getSender(), resp.getId(), resp.isVoting());

      for (Iterator<String> it = dbCtx.databases.listDBs().iterator(); it.hasNext();) {
        String db = it.next();

        log.info("Sync database {} to {}", db, getSender().path());
        final IOFile src = dbCtx.globalopts.dbpath(db);
        byte[] tbl = new byte[0];
        byte[] tbli = new byte[0];
        byte[] inf = new byte[0];
        byte[] txt = new byte[0];
        byte[] atv = new byte[0];
        // file content
        for(final String file : src.descendants()) {
          if(file.equals(DATATBL + IO.BASEXSUFFIX)) {
            tbl = getData(new IOFile(src, file));
          } else if(file.equals(DATATBL + "i" + IO.BASEXSUFFIX)) {
            tbli = getData(new IOFile(src, file));
          } else if(file.equals(DATAINF + IO.BASEXSUFFIX)) {
            inf = getData(new IOFile(src, file));
          } else if(file.equals(DATATXT + IO.BASEXSUFFIX)) {
            txt = getData(new IOFile(src, file));
          } else if(file.equals(DATAATV + IO.BASEXSUFFIX)) {
            atv = getData(new IOFile(src, file));
          }
        }

        getSender().tell(new DatabaseMessage(db, tbl, tbli, inf, txt, atv), getSelf());
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
