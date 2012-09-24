package org.basex.gui.view.editor;

import static org.basex.core.Text.*;
import static org.basex.gui.GUIConstants.*;
import static org.basex.util.Token.*;

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;

import javax.swing.*;
import javax.swing.event.*;

import org.basex.data.*;
import org.basex.gui.*;
import org.basex.gui.GUIConstants.Fill;
import org.basex.gui.GUIConstants.Msg;
import org.basex.gui.editor.*;
import org.basex.gui.editor.Editor.Action;
import org.basex.gui.layout.*;
import org.basex.gui.layout.BaseXFileChooser.Mode;
import org.basex.gui.layout.BaseXLayout.DropHandler;
import org.basex.gui.view.*;
import org.basex.io.*;
import org.basex.util.*;
import org.basex.util.list.*;

/**
 * This view allows the input and evaluation of queries and documents.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class EditorView extends View implements EditorNotifier {
  /** Error string. */
  private static final String ERRSTRING = STOPPED_AT + ' ' +
      (LINE_X + ", " + COLUMN_X).replaceAll("%", "([0-9]+)");
  /** XQuery error pattern. */
  private static final Pattern XQERROR =
    Pattern.compile(ERRSTRING + ' ' + IN_FILE_X.replaceAll("%", "(.*?)") + COL);
  /** XML error pattern. */
  private static final Pattern XMLERROR =
    Pattern.compile(LINE_X.replaceAll("%", "(.*?)") + COL + ".*");

  /** Execute Button. */
  final BaseXButton stop;
  /** Info label. */
  final BaseXLabel info;
  /** Position label. */
  final BaseXLabel pos;
  /** Query area. */
  final BaseXTabs tabs;
  /** Execute button. */
  final BaseXButton go;
  /** Thread counter. */
  int threadID;

  /** File in which the most recent error occurred. */
  String errFile;
  /** Most recent error position. */
  int errPos;

  /** Header string. */
  private final BaseXLabel header;
  /** Filter button. */
  private final BaseXButton filter;

  /** Search panel. */
  public final SearchPanel search;

  /**
   * Default constructor.
   * @param man view manager
   */
  public EditorView(final ViewNotifier man) {
    super(EDITORVIEW, man);

    border(6, 6, 6, 6).layout(new BorderLayout(0, 2)).setFocusable(false);

    header = new BaseXLabel(EDITOR, true, false);

    final BaseXButton openB = BaseXButton.command(GUICommands.C_EDITOPEN, gui);
    final BaseXButton saveB = new BaseXButton(gui, "save", H_SAVE);
    final BaseXButton hist = new BaseXButton(gui, "hist", H_RECENTLY_OPEN);
    final BaseXButton srch = new BaseXButton(gui, "search", H_REPLACE);

    final BaseXBack buttons = new BaseXBack(Fill.NONE);
    buttons.layout(new TableLayout(1, 4, 1, 0));
    buttons.add(srch);
    buttons.add(openB);
    buttons.add(saveB);
    buttons.add(hist);

    final BaseXBack b = new BaseXBack(Fill.NONE).layout(new BorderLayout(8, 0));
    b.add(header, BorderLayout.CENTER);
    b.add(buttons, BorderLayout.EAST);
    add(b, BorderLayout.NORTH);

    final BaseXBack center = new BaseXBack(Fill.NONE).layout(new BorderLayout(0, 2));
    tabs = new BaseXTabs(gui);
    tabs.setFocusable(false);
    search = new SearchPanel(gui, this, true);
    addCreateTab();
    final EditorArea edit = addTab();

    center.add(tabs, BorderLayout.CENTER);
    center.add(search, BorderLayout.SOUTH);
    add(center, BorderLayout.CENTER);
    search.setVisible(false);

    // status and query pane
    info = new BaseXLabel().setText(OK, Msg.SUCCESS);
    pos = new BaseXLabel(" ");
    pos.setText(edit.pos());

    stop = new BaseXButton(gui, "stop", H_STOP_PROCESS);
    stop.addKeyListener(this);
    stop.setEnabled(false);

    go = new BaseXButton(gui, "go", H_EXECUTE_QUERY);
    go.addKeyListener(this);

    filter = BaseXButton.command(GUICommands.C_FILTER, gui);
    filter.addKeyListener(this);

    final BaseXBack status = new BaseXBack(Fill.NONE).layout(new BorderLayout(4, 0));
    status.add(info, BorderLayout.CENTER);
    status.add(pos, BorderLayout.EAST);

    final BaseXBack query = new BaseXBack(Fill.NONE).layout(new TableLayout(1, 3, 1, 0));
    query.add(stop);
    query.add(go);
    query.add(filter);

    final BaseXBack south = new BaseXBack(Fill.NONE).border(4, 0, 0, 0);
    south.layout(new BorderLayout(8, 0));
    south.add(status, BorderLayout.CENTER);
    south.add(query, BorderLayout.EAST);
    add(south, BorderLayout.SOUTH);

    refreshLayout();

    // add listeners
    srch.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent e) {
        search.activate(true);
      }
    });
    saveB.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent e) {
        final JPopupMenu pop = new JPopupMenu();
        final StringBuilder mnem = new StringBuilder();
        final JMenuItem sa = GUIMenu.newItem(GUICommands.C_EDITSAVE, gui, mnem);
        final JMenuItem sas = GUIMenu.newItem(GUICommands.C_EDITSAVEAS, gui, mnem);
        GUICommands.C_EDITSAVE.refresh(gui, sa);
        GUICommands.C_EDITSAVEAS.refresh(gui, sas);
        pop.add(sa);
        pop.add(sas);
        pop.show(saveB, 0, saveB.getHeight());
      }
    });
    hist.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent e) {
        final JPopupMenu popup = new JPopupMenu();
        final ActionListener al = new ActionListener() {
          @Override
          public void actionPerformed(final ActionEvent ac) {
            open(new IOFile(ac.getActionCommand()));
          }
        };
        if(gui.gprop.strings(GUIProp.EDITOR).length == 0) {
          popup.add(new JMenuItem("- No recently opened files -"));
        }
        for(final String en : gui.gprop.strings(GUIProp.EDITOR)) {
          final JMenuItem jmi = new JMenuItem(en);
          jmi.addActionListener(al);
          popup.add(jmi);
        }
        popup.show(hist, 0, hist.getHeight());
      }
    });
    info.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseClicked(final MouseEvent e) {
        EditorArea ea = getEditor();
        if(errFile != null) {
          ea = find(IO.get(errFile), false);
          if(ea == null) ea = open(new IOFile(errFile));
          tabs.setSelectedComponent(ea);
        }
        if(errPos == -1) return;
        ea.jumpError(errPos);
        pos.setText(ea.pos());
      }
    });
    stop.addActionListener(new ActionListener() {
      @Override
     public void actionPerformed(final ActionEvent e) {
        stop.setEnabled(false);
        go.setEnabled(false);
        info.setText(OK, Msg.SUCCESS);
        gui.stop();
      }
    });
    go.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent e) {
        getEditor().release(Action.EXECUTE);
      }
    });
    tabs.addChangeListener(new ChangeListener() {
      @Override
      public void stateChanged(final ChangeEvent e) {
        final EditorArea ea = getEditor();
        if(ea == null) return;
        search.search();
        gui.refreshControls();
        pos.setText(ea.pos());
        ea.release(Action.PARSE);
        refreshMark();
      }
    });

    BaseXLayout.addDrop(this, new DropHandler() {
      @Override
      public void drop(final Object file) {
        if(file instanceof File) open(new IOFile((File) file));
      }
    });
  }

  @Override
  public void refreshInit() { }

  @Override
  public void refreshFocus() { }

  @Override
  public void refreshMark() {
    final EditorArea edit = getEditor();
    go.setEnabled(edit.script || edit.xquery && !gui.gprop.is(GUIProp.EXECRT));
    final Nodes marked = gui.context.marked;
    filter.setEnabled(!gui.gprop.is(GUIProp.FILTERRT) &&
        marked != null && marked.size() != 0);
  }

  @Override
  public void refreshContext(final boolean more, final boolean quick) { }

  @Override
  public void refreshLayout() {
    header.setFont(GUIConstants.lfont);
    for(final EditorArea edit : editors()) edit.setFont(GUIConstants.mfont);
    refreshMark();
    search.refreshLayout();
  }

  @Override
  public void refreshUpdate() { }

  @Override
  public boolean visible() {
    return gui.gprop.is(GUIProp.SHOWEDITOR);
  }

  @Override
  public void visible(final boolean v) {
    gui.gprop.set(GUIProp.SHOWEDITOR, v);
  }

  @Override
  protected boolean db() {
    return false;
  }

  /**
   * Opens a new file.
   */
  public void open() {
    // open file chooser for XML creation
    final BaseXFileChooser fc = new BaseXFileChooser(OPEN,
        gui.gprop.get(GUIProp.WORKPATH), gui);
    fc.filter(BXS_FILES, IO.BXSSUFFIX);
    fc.filter(XQUERY_FILES, IO.XQSUFFIXES);
    fc.filter(XML_DOCUMENTS, IO.XMLSUFFIXES);

    final IOFile[] files = fc.multi().selectAll(Mode.FOPEN);
    for(final IOFile f : files) open(f);
  }

  /**
   * Saves the contents of the currently opened editor.
   * @return {@code false} if operation was canceled
   */
  public boolean save() {
    final EditorArea edit = getEditor();
    if(!edit.opened()) return saveAs();
    save(edit.file());
    return true;
  }

  /**
   * Saves the contents of the currently opened editor under a new name.
   * @return {@code false} if operation was canceled
   */
  public boolean saveAs() {
    // open file chooser for XML creation
    final EditorArea edit = getEditor();
    final BaseXFileChooser fc = new BaseXFileChooser(
        SAVE_AS, edit.file().path(), gui).filter(XQUERY_FILES, IO.XQSUFFIXES);

    final IOFile file = fc.select(Mode.FSAVE);
    if(file == null) return false;
    save(file);
    return true;
  }

  /**
   * Creates a new file.
   */
  public void newFile() {
    addTab();
    refresh(true);
  }

  /**
   * Opens the specified query file.
   * @param file query file
   * @return opened editor
   */
  public EditorArea open(final IOFile file) {
    if(!visible()) GUICommands.C_SHOWEDITOR.execute(gui);

    EditorArea edit = find(file, true);
    try {
      if(edit != null) {
        // switch to open file
        tabs.setSelectedComponent(edit);
        // check if file in memory was modified, and save it if necessary
        if(!confirm(edit)) return edit;
        edit.setText(file.read());
      } else {
        // get current editor
        edit = getEditor();
        // create new tab if current text is stored on disk or has been modified
        if(edit.opened() || edit.modified) edit = addTab();
        edit.file(file);
        edit.initText(file.read());
      }

      // update file history and refresh the file modification
      gui.gprop.recent(file);
      refresh(true);
      edit.release(Action.PARSE);

    } catch(final IOException ex) {
      BaseXDialog.error(gui, FILE_NOT_OPENED);
    }
    return edit;
  }

  /**
   * Closes an editor.
   * @param edit editor to be closed. {@code null} closes the currently
   * opened editor.
   * opened editor is to be closed
   */
  public void close(final EditorArea edit) {
    final EditorArea ea = edit != null ? edit : getEditor();
    if(!confirm(ea)) return;

    tabs.remove(ea);
    final int t = tabs.getTabCount();
    final int i = tabs.getSelectedIndex();
    if(t == 1) {
      // reopen single tab
      addTab();
    } else if(i + 1 == t) {
      // if necessary, activate last editor tab
      tabs.setSelectedIndex(i - 1);
    }
  }

  /**
   * Initializes the info message.
   */
  public void reset() {
    ++threadID;
    errFile = null;
    info.setToolTipText(null);
    info.setText(OK, Msg.SUCCESS);
    stop.setEnabled(false);
  }

  /**
   * Starts a thread, which shows a waiting info after a short timeout.
   */
  public void start() {
    final int thread = threadID;
    new Thread() {
      @Override
      public void run() {
        Performance.sleep(200);
        if(thread == threadID) {
          info.setText(PLEASE_WAIT_D, Msg.SUCCESS).setToolTipText(null);
          stop.setEnabled(true);
        }
      }
    }.start();
  }

  /**
   * Evaluates the info message resulting from a query execution.
   * @param msg info message
   * @param ok true if query was successful
   */
  public void info(final String msg, final boolean ok) {
    ++threadID;
    errPos = -1;
    errFile = null;
    info.setCursor(!ok && error(msg) ?
        GUIConstants.CURSORHAND : GUIConstants.CURSORARROW);
    final String m = msg.replaceAll("^.*\\r?\\n\\[.*?\\]", "").
        replaceAll(".*" + LINE_X.replaceAll("%", ".*?") + COL, "");
    info.setText(m, ok ? Msg.SUCCESS : Msg.ERROR).setToolTipText(ok ? null : msg);
    stop.setEnabled(false);
    refreshMark();
  }

  /**
   * Handles info messages resulting from a query execution.
   * @param msg info message
   * @return true if error was found
   */
  private boolean error(final String msg) {
    final String line = msg.replaceAll("[\\r\\n].*", "");
    Matcher m = XQERROR.matcher(line);
    int el = 0, ec = 2;
    if(!m.matches()) {
      m = XMLERROR.matcher(line);
      if(!m.matches()) return true;
      el = Integer.parseInt(m.group(1));
      errFile = getEditor().file.path();
    } else {
      el = Integer.parseInt(m.group(1));
      ec = Integer.parseInt(m.group(2));
      errFile = m.group(3);
    }

    final EditorArea edit = find(IO.get(errFile), false);
    if(edit == null) return true;

    // find approximate error position
    final int ll = edit.last.length;
    errPos = ll;
    for(int e = 1, l = 1, c = 1; e < ll; ++c, e += cl(edit.last, e)) {
      if(l > el || l == el && c == ec) {
        errPos = e;
        break;
      }
      if(edit.last[e] == '\n') {
        ++l;
        c = 0;
      }
    }
    edit.error(errPos);
    return errPos != -1;
  }

  /**
   * Shows a quit dialog for all modified query files.
   * @return {@code false} if confirmation was canceled
   */
  public boolean confirm() {
    for(final EditorArea edit : editors()) if(!confirm(edit)) return false;
    return true;
  }

  /**
   * Checks if the current text can be saved. The check returns {@code true}
   * if the text has not been opened from disk, or if it has been modified.
   * @return result of check
   */
  public boolean saveable() {
    final EditorArea edit = getEditor();
    return !edit.opened() || edit.modified;
  }

  @Override
  public EditorArea getEditor() {
    final Component c = tabs.getSelectedComponent();
    return c instanceof EditorArea ? (EditorArea) c : null;
  }

  /**
   * Refreshes the query modification flag.
   * @param force action
   */
  void refresh(final boolean force) {
    // update modification flag
    final EditorArea edit = getEditor();
    final boolean oe = edit.modified;
    edit.modified = edit.hist != null && edit.hist.modified();
    if(edit.modified == oe && !force) return;

    // update tab title
    String title = edit.file().name();
    if(edit.modified) title += '*';
    edit.label.setText(title);

    // update components
    gui.refreshControls();
    pos.setText(edit.pos());
    refreshMark();
  }

  /**
   * Finds the editor that contains the specified file.
   * @param file file to be found
   * @param opened considers only opened files
   * @return editor
   */
  EditorArea find(final IO file, final boolean opened) {
    for(final EditorArea edit : editors()) {
      if(edit.file().eq(file) && (!opened || edit.opened())) return edit;
    }
    return null;
  }

  /**
   * Saves the specified editor contents.
   * @param file file to write
   */
  private void save(final IOFile file) {
    try {
      final EditorArea edit = getEditor();
      file.write(edit.getText());
      edit.file(file);
      gui.gprop.recent(file);
      refresh(true);
    } catch(final IOException ex) {
      BaseXDialog.error(gui, FILE_NOT_SAVED);
    }
  }

  /**
   * Choose a unique tab file.
   * @return io reference
   */
  private IOFile newTabFile() {
    // collect numbers of existing files
    final BoolList bl = new BoolList();
    for(final EditorArea edit : editors()) {
      if(edit.opened()) continue;
      final String n = edit.file().name().substring(FILE.length());
      bl.set(n.isEmpty() ? 1 : Integer.parseInt(n), true);
    }
    // find first free file number
    int c = 0;
    while(++c < bl.size() && bl.get(c));
    // create io reference
    return new IOFile(gui.gprop.get(GUIProp.WORKPATH), FILE + (c == 1 ? "" : c));
  }

  /**
   * Adds a new editor tab.
   * @return editor reference
   */
  EditorArea addTab() {
    final EditorArea edit = new EditorArea(this, newTabFile());
    edit.setFont(GUIConstants.mfont);
    edit.setSearch(search);

    final BaseXBack tab = new BaseXBack(new BorderLayout(10, 0)).mode(Fill.NONE);
    tab.add(edit.label, BorderLayout.CENTER);

    final BaseXButton close = tabButton("e_close");
    close.setRolloverIcon(BaseXLayout.icon("e_close2"));
    close.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent e) {
        close(edit);
      }
    });
    tab.add(close, BorderLayout.EAST);

    tabs.add(edit, tab, tabs.getComponentCount() - 2);
    return edit;
  }

  /**
   * Adds a tab for creating new tabs.
   */
  private void addCreateTab() {
    final BaseXButton add = tabButton("e_new");
    add.setRolloverIcon(BaseXLayout.icon("e_new2"));
    add.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(final ActionEvent e) {
        addTab();
        refresh(true);
      }
    });
    tabs.add(new BaseXBack(), add, 0);
    tabs.setEnabledAt(0, false);
  }

  /**
   * Adds a new tab button.
   * @param icon button icon
   * @return button
   */
  private BaseXButton tabButton(final String icon) {
    final BaseXButton b = new BaseXButton(gui, icon, null);
    b.border(2, 2, 2, 2).setContentAreaFilled(false);
    b.setFocusable(false);
    return b;
  }

  /**
   * Shows a quit dialog for the specified editor.
   * @param edit editor to be saved
   * @return {@code false} if confirmation was canceled
   */
  private boolean confirm(final EditorArea edit) {
    if(edit.modified && (edit.opened() || edit.getText().length != 0)) {
      final Boolean ok = BaseXDialog.yesNoCancel(gui,
          Util.info(CLOSE_FILE_X, edit.file().name()));
      if(ok == null || ok && !save()) return false;
    }
    return true;
  }

  /**
   * Returns all editors.
   * @return editors
   */
  EditorArea[] editors() {
    final ArrayList<EditorArea> edits = new ArrayList<EditorArea>();
    for(final Component c : tabs.getComponents()) {
      if(c instanceof EditorArea) edits.add((EditorArea) c);
    }
    return edits.toArray(new EditorArea[edits.size()]);
  }
}
