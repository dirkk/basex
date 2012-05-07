package org.basex.gui.dialog;

import static org.basex.core.Text.*;

import java.awt.*;

import org.basex.core.*;
import org.basex.gui.*;
import org.basex.gui.GUIConstants.Msg;
import org.basex.gui.layout.*;

/**
 * Dialog window for opening or join a network of BaseX instances.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public final class DialogDistribution extends Dialog {
  /** "Create and join a cluster" dialog. */
  private BaseXBack pnl;
  /** Host name. */
  private final BaseXTextField hostIn;
  /** Port. */
  private final BaseXTextField portIn;
  /** Host name. */
  private final BaseXTextField hostOut;
  /** Port. */
  private final BaseXTextField portOut;
  /** A new network should be created. */
  private final BaseXCheckBox create;
  /** This peer will be a super-peer */
  private final BaseXCheckBox superPeer;
  /** Buttons. */
  private final BaseXBack buttons;
  /** User feedback. */
  private final BaseXLabel info;

  /**
   * Default constructor.
   * @param main reference to the main window
   */
  public DialogDistribution(final GUI main) {
    super(main, DIALOG_DISTRIBUTION);

    // define buttons first to assign simplest mnemonics
    buttons = okCancel();

    final MainProp mprop = gui.context.mprop;

    pnl = new BaseXBack(new TableLayout(7, 2));
    
    // Host name text field
    hostIn = new BaseXTextField(mprop.get(MainProp.P2PHOST), this);
    hostIn.addKeyListener(keys);
    pnl.add(new BaseXLabel(DISTRIBUTION_INCOMING_HOST + COLS, false, true).border(8, 0, 6, 0));
    pnl.add(new BaseXLabel(""));
    pnl.add(hostIn);
    
    // Port text field
    portIn = new BaseXTextField(mprop.get(MainProp.P2PPORT), this);
    portIn.addKeyListener(keys);
    pnl.add(portIn);
    
    create = new BaseXCheckBox(CREATE_NETWORK, true, this);
    create.addKeyListener(keys);
    pnl.add(create);
    pnl.add(new BaseXLabel(""));
    
    superPeer = new BaseXCheckBox(NEW_SUPERPEER, true, this);
    superPeer.addKeyListener(keys);
    pnl.add(superPeer);
    pnl.add(new BaseXLabel(""));
    
    // Host name text field
    hostOut = new BaseXTextField(mprop.get(MainProp.P2PHOST), this);
    hostOut.setEnabled(false);
    hostOut.addKeyListener(keys);
    pnl.add(new BaseXLabel(DISTRIBUTION_JOINING_NODE + COLS, false, true).border(8, 0, 6, 0));
    pnl.add(new BaseXLabel(""));
    pnl.add(hostOut);
    
    // Port text field
    portOut = new BaseXTextField(mprop.get(MainProp.P2PPORT), this);
    portOut.setEnabled(false);
    portOut.addKeyListener(keys);
    pnl.add(portOut);
    
    info = new BaseXLabel(" ");
    pnl.add(info);

    set(pnl, BorderLayout.CENTER);

    set(buttons, BorderLayout.SOUTH);

    finish(null);
  }

  @Override
  public void action(final Object comp) {
    String infoText = null;
    Msg icon = null;
    
    if (comp == create) {
      if (create.isSelected()) {
        hostOut.setEnabled(false);
        portOut.setEnabled(false);
      } else {
        hostOut.setEnabled(true);
        portOut.setEnabled(true);
      }
    } else {
      try {
        if (!(portIn.getText().matches("[\\d]+") &&
            Integer.parseInt(portIn.getText()) <= 65535)) {
          ok = false;
          infoText = "Port of the network node is invalid.";
          icon = Msg.ERROR;
        } else if (!(portOut.getText().matches("[\\d]+") &&
            Integer.parseInt(portOut.getText()) <= 65535)) {
          ok = false;
          infoText = "Port of the joining network node is invalid.";
          icon = Msg.ERROR;
        } else if (!(hostIn.getText().matches("([\\w]+://)?[\\w.-]+"))) {
          ok = false;
          infoText = "Host name of the network node is invalid.";
          icon = Msg.ERROR;
        } else if (!(hostOut.getText().matches("([\\w]+://)?[\\w.-]+"))) {
          ok = false;
          infoText = "Host name of the joining network node is invalid.";
          icon = Msg.ERROR;
        } else {
          ok = true;
        }
      } catch (NumberFormatException e) {
        
      }
      
      info.setText(infoText, icon);
      enableOK(buttons, B_OK, ok);
    }
  }

  @Override
  public void close() {
    if(!ok) return;
    
    super.close();
  }
}
