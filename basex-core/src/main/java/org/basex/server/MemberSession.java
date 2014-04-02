package org.basex.server;

import org.basex.core.*;
import org.basex.io.in.BufferInput;
import org.basex.io.in.DecodingInput;
import org.basex.io.out.ArrayOutput;
import org.basex.io.out.EncodingOutput;
import org.basex.io.out.PrintOutput;
import org.basex.util.Token;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This class offers methods to execute database commands via the
 * replication architecture. Commands are sent to the server instance over
 * a socket connection:
 * <ul>
 * <li> A socket instance is created by the constructor.</li>
 * <li> The {@link #execute} method sends database commands to the server.
 * All strings are encoded as UTF8 and suffixed by a zero byte.</li>
 * <li> If the command has been successfully executed, the result string is read.</li>
 * <li> Next, the command info string is read.</li>
 * <li> A last byte is next sent to indicate if command execution
 * was successful (0) or not (1).</li>
 * <li> {@link #close} closes the session by sending the {@link org.basex.core.parse.Commands.Cmd#EXIT}
 * command to the server.</li>
 * </ul>
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 * @author Dirk Kirsten
 */
public class MemberSession extends Session implements RemoteSession {
  /** Event notifications. */
  private final Map<String, EventNotifier> notifiers =
    Collections.synchronizedMap(new HashMap<String, EventNotifier>());
  /** Server output (buffered). */
  protected PrintOutput sout;
  /** Server input. */
  protected final InputStream sin;

  /** Socket reference. */
  private final Socket socket;

  /**
   * Constructor, specifying login data.
   * @param context database context
   * @param user user name
   * @param pass password
   * @param primary connect to the primary
   * @throws java.io.IOException I/O exception
   */
  @SuppressWarnings("unused")
  public MemberSession(final Context context, final String user, final String pass, final boolean primary)
      throws IOException {
    this(context, user, pass, primary, null);
  }

  /**
   * Constructor, specifying login data and an output stream.
   * @param context database context
   * @param user user name
   * @param pass password
   * @param primary connect to the primary
   * @param output client output; if set to {@code null}, results will
   * be returned as strings.
   * @throws java.io.IOException I/O exception
   */
  public MemberSession(final Context context, final String user, final String pass, final boolean primary,
                       final OutputStream output) throws IOException {
    this(context.globalopts.get(GlobalOptions.HOST),
         context.globalopts.get(GlobalOptions.PORT), user, pass, primary, output);
  }

  /**
   * Constructor, specifying the server host:port combination and login data.
   * @param host server name
   * @param port server port
   * @param user user name
   * @param pass password
   * @param primary connect to the primary
   * @throws java.io.IOException I/O exception
   */
  public MemberSession(final String host, final int port, final String user, final String pass, final boolean primary)
      throws IOException {
    this(host, port, user, pass, primary, null);
  }

  /**
   * Constructor, specifying the server host:port combination, login data and
   * an output stream.
   * @param host server name
   * @param port server port
   * @param user user name
   * @param pass password
   * @param primary connect to the primary
   * @param output client output; if set to {@code null}, results will
   * be returned as strings.
   * @throws java.io.IOException I/O exception
   */
  public MemberSession(final String host, final int port, final String user, final String pass,
                       final boolean primary, final OutputStream output) throws IOException {

    super(output);
    socket = new Socket();
    try {
      // limit timeout to five seconds
      socket.connect(new InetSocketAddress(host, port), 5000);
    } catch(final IllegalArgumentException ex) {
      throw new BaseXException(ex);
    }
    sin = socket.getInputStream();
    sout = PrintOutput.get(socket.getOutputStream());

    startAuthentication(user, pass, primary);
  }

  /**
   * Start authentication with the BaseX member instance.
   *
   * Authentication process works as follows:
   * <ul>
   *   <li>Client sends [(byte) protocol version][(String) username][String[] options, optional]</li>
   * </ul>
   *
   * @param user user name
   * @param pass password
   * @param primary connect to the primary
   * @throws IOException
   */
  private void startAuthentication(final String user, final String pass, final boolean primary) throws IOException {
    final BufferInput bi = new BufferInput(sin);
    String[] options = {"PRIMARY:" + primary};
    // send first message to server
    send(Replication.PROTOCOL_V1);
    send(user);
    send(options);
    sout.flush();

    if (bi.read() == 0) {
      // receive ok

      // receive timestamp
      final String ts = bi.readString();
      final int hashCode = bi.read();

      // respond with hashed password
      if (hashCode == Replication.HASH_MD5) {
        send(Token.md5(Token.md5(pass) + ts));
        sout.flush();
      }

      // wait for server authentication
      if (bi.read() == 0) {
        // login successful
        final String[] infos = readArray(bi);
        System.out.println("Auth successful, infos: " + infos);
      } else {
        // receive error, authentication aborted
        final int authErrCode = bi.read(); // error code number
        // options, if any
        final String[] authErrOptions = readArray(bi);

        if (authErrCode == 0) throw new LoginException();
        else System.out.println("Authentication Code: " + authErrCode + "\nOptions: " + authErrOptions);
      }
    } else {
      System.out.println("failed");
      // receive error, authentication aborted
      final int code = bi.read(); // error code number
      // options, if any
      final String[] errOptions = readArray(bi);
      System.out.println("Code: " + code + "\nOptions: " + errOptions);
    }
  }

  private String[] readArray(final BufferInput bi) throws IOException {
    int count = bi.read();
    String[] arr = new String[count];
    for (int i = 0; i < count; ++i) arr[i] = bi.readString();
    return arr;
  }

  @Override
  public void create(final String name, final InputStream input) throws IOException {
    send(ServerCmd.CREATE, input, name);
  }

  @Override
  public void add(final String path, final InputStream input) throws IOException {
    send(ServerCmd.ADD, input, path);
  }

  @Override
  public void replace(final String path, final InputStream input) throws IOException {
    send(ServerCmd.REPLACE, input, path);
  }

  @Override
  public void store(final String path, final InputStream input) throws IOException {
    send(ServerCmd.STORE, input, path);
  }

  @Override
  public ClientQuery query(final String query) throws IOException {
    return new ClientQuery(query, this, out);
  }

  @Override
  public synchronized void close() throws IOException {
    socket.close();
  }

  @Override
  protected void execute(final String cmd, final OutputStream os) throws IOException {
    send(cmd);
    sout.flush();
    receive(os);
  }

  @Override
  protected void execute(final Command cmd, final OutputStream os) throws IOException {
    execute(cmd.toString(), os);
  }

  /**
   * Watches an event.
   * @param name event name
   * @param notifier event notification
   * @throws java.io.IOException I/O exception
   */
  public void watch(final String name, final EventNotifier notifier) throws IOException {
    // TODO not supported
  }

  /**
   * Unwatches an event.
   * @param name event name
   * @throws java.io.IOException I/O exception
   */
  public void unwatch(final String name) throws IOException {
    // TODO not supported
  }

  /**
   * Sends the specified stream to the server.
   * @param input input stream
   * @throws java.io.IOException I/O exception
   */
  private void send(final InputStream input) throws IOException {
    final EncodingOutput eo = new EncodingOutput(sout);
    for(int b; (b = input.read()) != -1;) eo.write(b);
    sout.write(0);
    sout.flush();
    receive(null);
  }

  /**
   * Receives the info string.
   * @param os output stream to send result to. If {@code null}, no result
   *           will be requested
   * @throws java.io.IOException I/O exception
   */
  private void receive(final OutputStream os) throws IOException {
    final BufferInput bi = new BufferInput(sin);
    if(os != null) receive(bi, os);
    info = bi.readString();
    if(!ok(bi)) throw new BaseXException(info);
  }

  /**
   * Checks the next success flag.
   * @param bi buffer input
   * @return value of check
   * @throws java.io.IOException I/O exception
   */
  protected static boolean ok(final BufferInput bi) throws IOException {
    return bi.read() == 0;
  }

  /**
   * Sends the specified command, string arguments and input.
   * @param cmd command
   * @param input input stream
   * @param strings string arguments
   * @throws java.io.IOException I/O exception
   */
  void send(final ServerCmd cmd, final InputStream input, final String... strings)
      throws IOException {

    sout.write(cmd.code);
    for(final String s : strings) send(s);
    send(input);
  }

  /**
   * Retrieves data from the server.
   * @param bi buffered server input
   * @param os output stream
   * @throws java.io.IOException I/O exception
   */
  protected static void receive(final BufferInput bi, final OutputStream os)
      throws IOException {
    final DecodingInput di = new DecodingInput(bi);
    for(int b; (b = di.read()) != -1;) os.write(b);
  }

  /**
   * Sends a string to the server.
   * @param s string to be sent
   * @throws java.io.IOException I/O exception
   */
  public void send(final String s) throws IOException {
    sout.write(Token.token(s));
    sout.write(0);
  }

  /**
   * Sends a string array to the server.
   * @param arr string array to be sent
   * @throws java.io.IOException I/O exception
   */
  public void send(final String[] arr) throws IOException {
    send(arr.length);
    for (String s : arr) send(s);
  }

  public void send(final int i) throws IOException {
    sout.write(i);
  }

  public void send(final byte b) throws IOException {
    sout.write(b);
  }

  /**
   * Executes a command and sends the result to the specified output stream.
   * @param cmd server command
   * @param arg argument
   * @param os target output stream
   * @return string
   * @throws java.io.IOException I/O exception
   */
  public String exec(final ServerCmd cmd, final String arg, final OutputStream os) throws IOException {
    final OutputStream o = os == null ? new ArrayOutput() : os;
    sout.write(cmd.code);
    send(arg);
    sout.flush();
    final BufferInput bi = new BufferInput(sin);
    receive(bi, o);
    if(!ok(bi)) throw new BaseXException(bi.readString());
    return o.toString();
  }

  @Override
  public void execCache(ServerCmd cmd, String arg, Query q) throws IOException {
    sout.write(cmd.code);
    send(arg);
    sout.flush();
    final BufferInput bi = new BufferInput(sin);
    q.cache(bi);
    if(!ok(bi)) throw new BaseXException(bi.readString());
  }

  @Override
  public String toString() {
     return socket.getInetAddress().getHostAddress() + ':' + socket.getPort();
  }
}
