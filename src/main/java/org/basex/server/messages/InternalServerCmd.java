package org.basex.server.messages;

/**
 * This class defines the internal message commands of the server.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public enum InternalServerCmd {
  /** Start as master of a replica set. */
  STARTMASTER(0),
  /** Connect as slave to a master. */
  CONNECTMASTER(1),
  /** Unknown command. Should lead to a failure when used. */
  UNKNOWN(-1);

  /** Control code (soon obsolete). */
  public final int code;

  /**
   * Constructor.
   * @param c control code
   */
  InternalServerCmd(final int c) {
    code = c;
  }

  /**
   * Returns the server command for the specified control byte
   * (soon obsolete).
   * @param b control byte
   * @return server command
   */
  public static InternalServerCmd get(final int b) {
    for(final InternalServerCmd s : values()) if(s.code == b) return s;
    // current default for unknown codes: database command.
    return UNKNOWN;
  }
}
