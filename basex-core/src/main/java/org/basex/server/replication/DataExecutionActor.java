package org.basex.server.replication;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.basex.core.Command;
import org.basex.core.Context;
import org.basex.core.cmd.*;
import org.basex.util.Token;

import java.util.ArrayList;

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

  @Override
  public void onReceive(Object msg) throws Exception {
    if (msg instanceof DataMessage) {
      // got an update from the primary
      DataMessage data = (DataMessage) msg;

      // Document messages
      if (data instanceof AddMessage) {
        AddMessage addMessage = (AddMessage) data;

        String content = Token.string(addMessage.getContent());
        log.debug("Database: {}, Path: {}, Content: {}", addMessage.getDatabase(),
          addMessage.getPath(), content);

        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(addMessage.getDatabase()));
        cmds.add(new Add(addMessage.getPath(), content));
        execute(cmds);
      } else if (data instanceof UpdateMessage) {
        UpdateMessage updateMessage = (UpdateMessage) data;
        String content = Token.string(updateMessage.getContent());
        log.debug("Database: {}, Path: {}, Content: {}", updateMessage.getDatabaseName(),
          updateMessage.getPath(), content);


        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(updateMessage.getDatabaseName()));
        cmds.add(new Replace(updateMessage.getPath(), content));
        execute(cmds);
      } else if (data instanceof RenameMessage) {
        RenameMessage renameMessage = (RenameMessage) data;
        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(renameMessage.getDatabase()));
        cmds.add(new Rename(renameMessage.getSource(), renameMessage.getTarget()));
        execute(cmds);
      } else if (data instanceof DeleteMessage) {
        DeleteMessage deleteMessage = (DeleteMessage) data;

        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new Open(deleteMessage.getDatabase()));
        cmds.add(new Delete(((DeleteMessage) data).getTarget()));
        execute(cmds);
      }
      // Database messages
      else if (data instanceof CreateDbMessage) {
        ArrayList<Command> cmds = new ArrayList<Command>();
        cmds.add(new CreateDB(((CreateDbMessage) data).getName()));
        cmds.add(new Close());
        System.out.println(getSelf().path());
        execute(cmds);
      } else if (data instanceof AlterMessage) {
        AlterMessage am = (AlterMessage) data;
        new AlterDB(am.getSource(), am.getTarget()).execute(dbCtx);
      } else if (data instanceof DropMessage) {
        new DropDB(((DropMessage) data).getName()).execute(dbCtx);
      } else if (data instanceof CopyMessage) {
        CopyMessage cm = (CopyMessage) data;
        new Copy(cm.getSource(), cm.getTarget()).execute(dbCtx);
      }
    } else {
      unhandled(msg);
    }
  }
}
