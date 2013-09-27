package org.basex.server.replication;

import java.io.Serializable;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface DataMessages {
  public interface DataMessage {

  }

  public class AddMessage implements Serializable, DataMessage {
    private final String path;
    private final byte[] content;

    public AddMessage(final String path, final byte[] content) {
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

  public class UpdateMessage implements Serializable, DataMessage {
    private final String path;
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

  public class RenameMessage implements Serializable, DataMessage {
    private final String source;
    private final String target;

    public RenameMessage(String source, String target) {
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


  public class DeleteMessage implements Serializable, DataMessage {
    private final String target;

    public DeleteMessage(final String target) {
      this.target = target;
    }

    public String getTarget() {
      return target;
    }
  }

  public class CreateDbMessage implements Serializable, DataMessage {
    private final String name;

    public CreateDbMessage(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public class AlterMessage implements Serializable, DataMessage {
    private final String source;
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


  public class DropMessage implements Serializable, DataMessage {
    private final String name;

    public DropMessage(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }


  public class CopyMessage implements Serializable, DataMessage {
    private final String source;
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
