package org.basex.server;

import org.basex.BaseXServer;
import org.basex.core.Context;
import org.basex.core.MainProp;
import org.basex.util.Performance;
import org.basex.util.Util;

import java.io.IOException;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

import static org.basex.core.Text.ACCESS_DENIED;
import static org.basex.core.Text.TIMEOUT_EXCEEDED;
import static org.basex.util.Token.md5;

/**
 * Authenticates an user on the server side.
 *
 * @author Dirk Kirsten
 */
public class AuthenticationListener extends AListener {
  /** Timer for authentication time out. */
  public final Timer auth = new Timer();

  /**
   * Constructor.
   * @param s socket
   * @param c database context
   * @param srv server reference
   */
  public AuthenticationListener(final Socket s, final Context c, final BaseXServer srv) {
    super(s, c, srv);
    context = c;
    startTimeout();
  }

  /**
   * Start the authentication timeout timer.
   */
  private void startTimeout() {
    final long to = context.mprop.num(MainProp.KEEPALIVE) * 1000L;
    if(to > 0) {
      auth.schedule(new TimerTask() {
        @Override
        public void run() {
          quit();
        }
      }, to);
    }
  }



  /**
   * Initializes a session via cram-md5.
   * @return success flag
   */
  @Override
  public void run() {
    try {
      setStreams();

      final String ts = Long.toString(System.nanoTime());
      final byte[] address = socket.getInetAddress().getAddress();

      // send {TIMESTAMP}0
      out.print(ts);
      send(true);

      // evaluate login data
      // receive {USER}0{PASSWORD}0
      final String us = in.readString();
      final String pw = in.readString();
      context.user = context.users.get(us);
      running = context.user != null && md5(context.user.password + ts).equals(pw);

      // write log information
      if(running) {
        // send {OK}
        send(true);

        AListener listener = new ClientListener(socket, context, server);
        context.sessions.add((ClientListener) listener);

        listener.start();
        context.blocker.remove(address);
      } else {
        if(!us.isEmpty()) log(ACCESS_DENIED, false);
        // delay users with wrong passwords
        for(int d = context.blocker.delay(address); d > 0; d--) Performance.sleep(1000);
        send(false);
      }
    } catch(final IOException ex) {
      if(running) {
        Util.stack(ex);
        log(ex, false);
      }
    }
  }

  /**
   * Quits the authentication.
   */
  @Override
  public synchronized void quit() {
    try {
      socket.close();
      log(TIMEOUT_EXCEEDED, false);
    } catch(final Throwable ex) {
      log(ex, false);
    }
  }

}
