package org.basex.server;

import org.basex.BaseXServer;
import org.basex.core.Context;
import org.basex.io.in.BufferInput;
import org.basex.io.out.PrintOutput;
import org.basex.util.Performance;

import java.io.IOException;
import java.net.Socket;

import static org.basex.core.Text.COLS;

/**
 * Abstract Listener class.
 */
public abstract class AListener extends Thread {
  /** Timestamp of last interaction. */
  public long last;
  /** Database context. */
  protected Context context;
  /** Server reference. */
  protected final BaseXServer server;
  /** Socket reference. */
  protected final Socket socket;
  /** Input stream. */
  protected BufferInput in;
  /** Output stream. */
  protected PrintOutput out;
  /** Indicates if the server thread is running. */
  protected boolean running;

  /**
   * Constructor.
   * @param s socket
   * @param c database context
   * @param srv server reference
   */
  public AListener(final Socket s, final Context c, final BaseXServer srv) {
    socket = s;
    server = srv;
    last = System.currentTimeMillis();
    setDaemon(true);
  }

  /**
   * Sets the standard input and output streams.
   * @exception IOException I/O exception
   */
  protected void setStreams() throws IOException {
    out = PrintOutput.get(socket.getOutputStream());
    in = new BufferInput(socket.getInputStream());
  }

  /**
   * Returns the host and port of a client.
   * @return string representation
   */
  public String address() {
    return socket.getInetAddress().getHostAddress() + ':' + socket.getPort();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("[").append(address()).append(']');
    if(context.data() != null) sb.append(COLS).append(context.data().meta.name);
    return sb.toString();
  }

  /**
   * Returns error feedback.
   * @param info error string
   * @throws IOException I/O exception
   */
  protected void error(final String info) throws IOException {
    info(info, false);
  }

  /**
   * Returns user feedback.
   * @param info information string
   * @throws IOException I/O exception
   */
  protected void success(final String info) throws IOException {
    info(info, true);
  }

  /**
   * Returns user feedback.
   * @param info information string
   * @param ok success/error flag
   * @throws IOException I/O exception
   */
  protected void info(final String info, final boolean ok) throws IOException {
    // write feedback to log file
    log(info, ok);
    // send {MSG}0 and (0|1) as (success|error) flag
    out.writeString(info);
    send(ok);
  }

  /**
   * Sends a success flag to the client (0: true, 1: false).
   * @param ok success flag
   * @throws IOException I/O exception
   */
  protected void send(final boolean ok) throws IOException {
    out.write(ok ? 0 : 1);
    out.flush();
  }

  /**
   * Writes a log message.
   * @param info message info
   * @param type message type (true/false/null: OK, ERROR, REQUEST)
   */
  protected void log(final Object info, final Object type) {
    // add evaluation time if any type is specified
    final String user = context.user != null ? context.user.name : "";
    final Log log = context.log;
    if(log != null) log.write(new Object[] { address(), user, null, info });
  }

  public abstract void quit();
}
