package org.basex.gui.dialog;

import static org.basex.core.Text.*;

import java.awt.*;
import java.awt.event.*;

import javax.swing.*;

import org.basex.core.*;
import org.basex.gui.*;
import org.basex.gui.layout.*;
import org.basex.util.*;

/**
 * Dialog window for displaying the progress of a command execution.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class DialogProgress extends Dialog implements ActionListener {
  /** Maximum value of progress bar. */
  private static final int MAX = 600;
  /** Refresh action. */
  private final Timer timer = new Timer(100, this);
   /** Information label. */
  private BaseXLabel info;
  /** Memory usage. */
  private BaseXMem mem;
  /** Executed command. */
  private Command command;
  /** Progress bar. */
  private JProgressBar bar;

  /**
   * Default constructor.
   * @param main main window
   * @param title dialog title
   * @param cmd progress reference
   */
  private DialogProgress(final GUI main, final String title, final Command cmd) {
    super(main, title);
    init(main, cmd);
  }

  /**
   * Default constructor.
   * @param dialog dialog window
   * @param title dialog title
   * @param cmd progress reference
   */
  private DialogProgress(final Dialog dialog, final String title, final Command cmd) {
    super(dialog, title);
    init(dialog, cmd);
  }

  /**
   * Initializes all components.
   * @param parent parent component
   * @param cmd progress reference
   */
  private void init(final Component parent, final Command cmd) {
    info = new BaseXLabel(" ", true, true);
    set(info, BorderLayout.NORTH);

    if(cmd.supportsProg()) {
      bar = new JProgressBar(0, MAX);
      set(bar, BorderLayout.CENTER);
    } else {
      bar = null;
    }
    BaseXLayout.setWidth(info, MAX);

    final BaseXBack s = new BaseXBack(new BorderLayout()).border(10, 0, 0, 0);
    final BaseXBack m = new BaseXBack(new TableLayout(1, 2, 5, 0));
    mem = new BaseXMem(this, false);
    m.add(new BaseXLabel(MEMUSED_C));
    m.add(mem);
    s.add(m, BorderLayout.WEST);

    if(cmd.stoppable()) {
      final BaseXButton cancel = new BaseXButton(B_CANCEL, this);
      s.add(cancel, BorderLayout.EAST);
    }
    set(s, BorderLayout.SOUTH);

    command = cmd;
    timer.start();
    pack();
    setLocationRelativeTo(parent);
  }

  @Override
  public void cancel() {
    command.stop();
    close();
  }

  @Override
  public void close() {
    dispose();
  }

  @Override
  public void dispose() {
    timer.stop();
    super.dispose();
  }

  @Override
  public void actionPerformed(final ActionEvent e) {
    setTitle(command.title());
    final String detail = command.detail();
    info.setText(detail.isEmpty() ? " " : detail);
    mem.repaint();
    if(bar != null) bar.setValue((int) (command.progress() * MAX));
  }

  /**
   * Runs the specified commands, decorated by a progress dialog, and
   * calls {@link Dialog#action} if the dialog is closed.
   * @param dialog reference to the dialog window
   * @param title dialog title (may be an empty string)
   * @param cmds commands to be run
   */
  public static void execute(final Dialog dialog, final String title,
      final Command... cmds) {
    execute(dialog, title, null, cmds);
  }

  /**
   * Runs the specified commands, decorated by a progress dialog, and
   * calls {@link Dialog#action} if the dialog is closed.
   * @param dialog reference to the dialog window
   * @param title dialog title (may be an empty string)
   * @param post post-processing step
   * @param cmds commands to be run
   */
  public static void execute(final Dialog dialog, final String title, final Runnable post,
      final Command... cmds) {

    final GUI gui = dialog.gui;
    for(final Command cmd : cmds) {
      // reset views
      final boolean newData = cmd.newData(gui.context);
      if(newData) gui.notify.init();

      // create wait dialog
      final DialogProgress wait;
      if(dialog.isVisible()) {
        wait = new DialogProgress(dialog, title, cmd);
      } else {
        wait = new DialogProgress(gui, title, cmd);
      }

      // start command thread
      new Thread() {
        @Override
        public void run() {
          // execute command
          final Performance perf = new Performance();
          gui.updating = cmd.updating(gui.context);
          final boolean ok = cmd.run(gui.context);
          gui.updating = false;
          final String info = cmd.info();

          // return status information
          final String time = perf.toString();
          gui.info.setInfo(info, cmd, time, ok);
          gui.info.reset();
          gui.status.setText(Util.info(TIME_NEEDED_X, time));

          // close progress window and show error if command failed
          wait.dispose();
          if(!ok) Dialog.error(gui, info.equals(INTERRUPTED) ? COMMAND_CANCELED : info);
        }
      }.start();

      // show progress windows until being disposed
      wait.setVisible(true);

      // initialize views if database was closed before
      if(newData) gui.notify.init();
      else if(cmd.updating(gui.context)) gui.notify.update();
    }

    dialog.setEnabled(true);
    dialog.action(dialog);
    if(post != null) post.run();
  }
}
