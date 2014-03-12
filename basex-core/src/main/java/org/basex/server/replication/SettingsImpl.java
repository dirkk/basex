package org.basex.server.replication;

import akka.actor.Extension;
import com.typesafe.config.Config;

/**
 * Replication specific setting, which are stored in the {@literal application.conf} of
 * the akka configuration.
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class SettingsImpl implements Extension {
  /** Voting or Non-voting member. */
  public final boolean VOTING;
  /** Weight of thide node in an election. */
  public final int WEIGHT;

  public SettingsImpl(Config cfg) {
    VOTING = cfg.getBoolean("BaseX.voting");
    WEIGHT = cfg.getInt("BaseX.weight");
  }
}
