package org.basex.server.messages;

import java.io.*;

/**
 * An internal command message containing an internal Server command
 * and a list of arguments
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class ServerCommandMessage {
  /** Command to execute. */
  private final InternalServerCmd cmd;
  /** Arguments. */
  private final Serializable[] args;

  /**
   * Constructor.
   * @param c command to execute
   * @param a list of arguments
   */
  public ServerCommandMessage(final InternalServerCmd c, final Serializable... a) {
    cmd = c;
    args = a;
  }
  
  /**
   * Get the command.
   * @return command
   */
  public InternalServerCmd getCommand() {
    return cmd;
  }
  
  /**
   * Get all arguments provided.
   * @return list of arguments
   */
  public Serializable[] getArgs() {
    return args;
  }
}
