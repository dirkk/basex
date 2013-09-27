package org.basex.server.replication;

import org.basex.core.Replication;
import org.basex.io.out.ArrayOutput;
import org.basex.io.serial.Serializer;
import org.basex.query.value.node.DBNode;
import org.basex.trigger.DatabaseTrigger;
import org.basex.trigger.DocumentTrigger;

import java.io.IOException;

import static org.basex.server.replication.DataMessages.*;

/**
 * A trigger for document and database related events, i.e. all possible write operationbs. It is used
 * to propagate write operations to secondaries within a replica set.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ReplicationTrigger implements DocumentTrigger, DatabaseTrigger {
  /** Replication instance. */
  private final Replication repl;

  public ReplicationTrigger(final Replication r) {
    this.repl = r;
  }

  private String getPath(final DBNode node) {
    byte[] b = node.baseURI();
    return new String(b);
  }

  private byte[] getData(final DBNode node) {
    final ArrayOutput ao = new ArrayOutput();
    final Serializer ser;
    try {
      ser = Serializer.get(ao, null);
      ser.serialize(node);
      ser.close();

      byte[] t = ao.toArray();
      ao.close();
      return t;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public void afterAdd(final DBNode node) {
    repl.publish(new AddMessage(getPath(node), getData(node)));
  }

  @Override
  public void afterUpdate(DBNode node) {
    repl.publish(new UpdateMessage(getPath(node), getData(node)));
  }

  @Override
  public void afterDelete(String path) {
    repl.publish(new DeleteMessage(path));
  }

  @Override
  public void afterRename(final String source, final String target) {
    repl.publish(new RenameMessage(source, target));
  }

  @Override
  public void afterCreateDb(final String name) {
    repl.publish(new CreateDbMessage(name));

  }

  @Override
  public void afterAlter(final String source, final String target) {
    repl.publish(new AlterMessage(source, target));
  }

  @Override
  public void afterDrop(final String name) {
    repl.publish(new DropMessage(name));
  }

  @Override
  public void afterCopy(final String source, final String target) {
    repl.publish(new CopyMessage(source, target));
  }
}
