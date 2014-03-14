package org.basex.server.replication;

import akka.actor.Extension;
import akka.util.Timeout;
import com.typesafe.config.Config;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

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
  /** Standard timeout for operations. */
  public final Timeout TIMEOUT;

  public SettingsImpl(Config cfg) {
    VOTING = cfg.hasPath("BaseX.voting") ? cfg.getBoolean("BaseX.voting") : true;
    WEIGHT = cfg.hasPath("BaseX.weight") ? cfg.getInt("BaseX.weight") : 1;
    TIMEOUT = Timeout.durationToTimeout(
      Duration.create(cfg.hasPath("BaseX.timeout") ? cfg.getMilliseconds("BaseX.timeout") : 5000, TimeUnit.MILLISECONDS)
    );
  }
}
