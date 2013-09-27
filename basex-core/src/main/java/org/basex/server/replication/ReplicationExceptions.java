package org.basex.server.replication;

/**
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface ReplicationExceptions {
  /**
   * This exception is thrown if a command is run which tries to start up
   * a new replication system, although one is currently already running.
   */
  public class ReplicationAlreadyRunningException extends Throwable {

  }
}
