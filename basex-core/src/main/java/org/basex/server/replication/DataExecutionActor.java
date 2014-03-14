package org.basex.server.replication;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.basex.core.Command;
import org.basex.core.Context;
import org.basex.core.cmd.*;
import org.basex.io.IO;
import org.basex.io.IOFile;
import org.basex.io.out.DataOutput;
import org.basex.util.Token;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import static org.basex.data.DataText.*;
import static org.basex.server.replication.DataMessages.*;

/**
 * Executes BaseX database command, which can be blocking. Thus, this actor
 * should always be encapsulated within a router.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class DataExecutionActor extends UntypedActor {
  /** Database Context. */
  private Context dbCtx;
  /** Logging. */
  private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

  /**
   * Create props for an actor of this type.
   *
   * @return Props, can be further configured
   */
  public static Props mkProps(final Context dbContext) {
    return Props.create(DataExecutionActor.class, dbContext);
  }

  public DataExecutionActor(final Context dbCtx) {
    this.dbCtx = dbCtx;
  }

  private void execute(ArrayList<Command> cmds) {
    new ReplCmd(cmds, dbCtx).run();
  }

  /**
   * Copies a file to another target.
   * @param target target
   * @throws java.io.IOException I/O exception
   */
  public void copy(final byte[] in, final File target) throws IOException {
    DataOutput out = new DataOutput(new FileOutputStream(target));
    out.writeBytes(in);
    out.flush();
  }

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof DataMessage) {
      // got an update from the primary
      DataMessage dataMsg = (DataMessage) msg;

      // Document messages
      if (dataMsg instanceof AddMessage) {
        AddMessage addMessage = (AddMessage) dataMsg;

        String content = Token.string(addMessage.getContent());
        log.debug("Database: {}, Path: {}, Content: {}", addMessage.getDatabase(),
          addMessage.getPath(), content);

        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(addMessage.getDatabase()));
        cmds.add(new Add(addMessage.getPath(), content));
        execute(cmds);
      } else if (dataMsg instanceof UpdateMessage) {
        UpdateMessage updateMessage = (UpdateMessage) dataMsg;
        String content = Token.string(updateMessage.getContent());
        log.debug("Database: {}, Path: {}, Content: {}", updateMessage.getDatabaseName(),
          updateMessage.getPath(), content);


        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(updateMessage.getDatabaseName()));
        cmds.add(new Replace(updateMessage.getPath(), content));
        execute(cmds);
      } else if (dataMsg instanceof RenameMessage) {
        RenameMessage renameMessage = (RenameMessage) dataMsg;
        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(renameMessage.getDatabase()));
        cmds.add(new Rename(renameMessage.getSource(), renameMessage.getTarget()));
        execute(cmds);
      } else if (dataMsg instanceof DeleteMessage) {
        DeleteMessage deleteMessage = (DeleteMessage) dataMsg;

        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(deleteMessage.getDatabase()));
        cmds.add(new Delete(((DeleteMessage) dataMsg).getTarget()));
        execute(cmds);
      }
        // Database messages
        else if (dataMsg instanceof CreateDbMessage) {
        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new CreateDB(((CreateDbMessage) dataMsg).getName()));
        cmds.add(new Close());
        System.out.println(getSelf().path());
        execute(cmds);
      } else if (dataMsg instanceof AlterMessage) {
        AlterMessage am = (AlterMessage) dataMsg;
        new AlterDB(am.getSource(), am.getTarget()).execute(dbCtx);
      } else if (dataMsg instanceof DropMessage) {
        new DropDB(((DropMessage) dataMsg).getName()).execute(dbCtx);
      } else if (dataMsg instanceof CopyMessage) {
        CopyMessage cm = (CopyMessage) dataMsg;
        new Copy(cm.getSource(), cm.getTarget()).execute(dbCtx);
      } else if (dataMsg instanceof DatabaseMessage) {
        DatabaseMessage dbMsg = (DatabaseMessage) dataMsg;

        // close open database, if any
        new Close().run(dbCtx);

        // drop old database, if any
        DropDB.drop(dbMsg.getName(), dbCtx);

        // add files
        String dir = dbCtx.globalopts.get(dbCtx.globalopts.DBPATH) + "/" + dbMsg.getName();
        new IOFile(new File(dir, DATATBL + IO.BASEXSUFFIX)).dir().md();
        copy(dbMsg.getTbl(), new File(dir, DATATBL + IO.BASEXSUFFIX));
        copy(dbMsg.getTbli(), new File(dir, DATATBL + "i" + IO.BASEXSUFFIX));
        copy(dbMsg.getInf(), new File(dir, DATAINF + IO.BASEXSUFFIX));
        copy(dbMsg.getTxt(), new File(dir, DATATXT + IO.BASEXSUFFIX));
        copy(dbMsg.getAtv(), new File(dir, DATAATV + IO.BASEXSUFFIX));

        new Open(dbMsg.getName()).execute(dbCtx);
        // optimize the database to create index structures
        new Optimize().run(dbCtx);

        // close the database
        new Close().run(dbCtx);
      }
    } else {
      unhandled(msg);
    }
  }
}
