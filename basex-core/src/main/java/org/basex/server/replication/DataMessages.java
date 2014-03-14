package org.basex.server.replication;

import java.io.Serializable;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface DataMessages {
  /**
   * Generic interface for all messages which aim to replicate an updating
   * operation from a Primary to a Secondary.
   */
  public interface DataMessage {
  }

  public class DatabaseMessage implements Serializable, DataMessage {
    /** Database name. */
    private final String name;

    private final byte[] tbl;
    private final byte[] tbli;
    private final byte[] inf;
    private final byte[] txt;
    private final byte[] atv;

    public DatabaseMessage(final String name, final byte[] tbl, final byte[] tbli, final byte[] inf, final byte[] txt,
                           final byte[] atv) {
      this.name = name;
      this.tbl = tbl;
      this.tbli = tbli;
      this.inf = inf;
      this.txt = txt;
      this.atv = atv;
    }

    public String getName() {
      return name;
    }

    public byte[] getTbl() { return tbl; }

    public byte[] getTbli() { return tbli; }

    public byte[] getInf() {
      return inf;
    }

    public byte[] getTxt() {
      return txt;
    }

    public byte[] getAtv() {
      return atv;
    }
  }

  /**
   * Add a document to a database.
   */
  public class AddMessage implements Serializable, DataMessage {
    /** Document path. */
    private final String path;
    /** Serialized document content. */
    private final byte[] content;

    public AddMessage(final String path, final byte[] content) {
      this.path = path;
      this.content = content;
    }

    public String getPath() {
      return path;
    }

    public String getDatabase() {
      return getPath().substring(0, getPath().indexOf("/"));
    }

    public byte[] getContent() {
      return content;
    }
  }

  /**
   * An existing document was updated using XQuery Update. The whole
   * document is serialiuzed and replicated to all Secondaries within
   * the replica set.
   */
  public class UpdateMessage implements Serializable, DataMessage {
    /** Document path. */
    private final String path;
    /** Serialized document content. */
    private final byte[] content;

    public UpdateMessage(final String path, final byte[] content) {
      this.path = path;
      this.content = content;
    }

    public String getPath() {
      return path;
    }

    public String getDatabaseName() {
      return getPath().substring(0, getPath().indexOf("/"));
    }

    public byte[] getContent() {
      return content;
    }
  }

  /**
   * A document was renamed.
   */
  public class RenameMessage implements Serializable, DataMessage {
    /** Old document path. */
    private final String source;
    /** New document path. */
    private final String target;
    /** Database name. */
    private final String database;

    public RenameMessage(String source, String target, String database) {
      this.source = source;
      this.target = target;
      this.database = database;
    }

    public String getSource() {
      return source;
    }

    public String getTarget() {
      return target;
    }

    public String getDatabase() {
      return database;
    }
  }

  /**
   * A document was removed.
   */
  public class DeleteMessage implements Serializable, DataMessage {
    /** Document path to be removed. */
    private final String target;
    /** Database name. */
    private final String database;

    public DeleteMessage(final String target, String database) {
      this.target = target;
      this.database = database;
    }

    public String getTarget() {
      return target;
    }

    public String getDatabase() {
      return database;
    }
  }

  /**
   * A new database is being created.
   */
  public class CreateDbMessage implements Serializable, DataMessage {
    /** Name of the database. */
    private final String name;

    public CreateDbMessage(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * A database is being renamed.
   */
  public class AlterMessage implements Serializable, DataMessage {
    /** Old database name. */
    private final String source;
    /** New database name. */
    private final String target;

    public AlterMessage(String source, String target) {
      this.source = source;
      this.target = target;
    }

    public String getSource() {
      return source;
    }

    public String getTarget() {
      return target;
    }
  }

  /**
   * A database is being deleted.
   */
  public class DropMessage implements Serializable, DataMessage {
    /** Database to deleted. */
    private final String name;

    public DropMessage(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  /**
   * A database is being copied.
   */
  public class CopyMessage implements Serializable, DataMessage {
    /** Name of the database to copy from. */
    private final String source;
    /** New database name of the database to be copied into. */
    private final String target;

    public CopyMessage(String source, String target) {
      this.source = source;
      this.target = target;
    }

    public String getSource() {
      return source;
    }

    public String getTarget() {
      return target;
    }
  }
}
