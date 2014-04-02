package org.basex.server.replication;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.RootActorPath;
import org.basex.util.Prop;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import static org.basex.server.replication.ReplicaSet.ReplicaSetState;

/**
 * Replica set messages for internal communication..
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public interface InternalMessages {
  public class StatusMessage implements Serializable {
    /** Cluster state. */
    private final ReplicaSetState state;
    /** Primary member. Null, if there is no primary. */
    private final Member primary;
    /** All secondary members. */
    private final List<Member> secondaries;

    /**
     * Replica set status message. Contains the current state and members of the replica set.
     *
     * @param state state of the cluster
     * @param primary primary, if any
     * @param secondaries all secondaries
     */
    public StatusMessage(ReplicaSetState state, Member primary, List<Member> secondaries) {
      this.state = state;
      this.primary = primary;
      this.secondaries = secondaries;
    }

    /**
     * Replica set status message. Contains the current state and members of the replica set.
     *
     * @param set replica set
     */
    public StatusMessage(ReplicaSet set) {
      this.state = set.getState();
      this.primary = set.getPrimary();
      this.secondaries = set.getSecondaries();
    }

    public ReplicaSetState getState() {
      return state;
    }

    public Member getPrimary() {
      return primary;
    }

    public List<Member> getSecondaries() {
      return secondaries;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append("State: " + getState() + Prop.NL);
      b.append("Primary" + Prop.NL + getPrimary() + Prop.NL);
      b.append("Number of secondaries: " + getSecondaries().size() + Prop.NL);
      for (Iterator<Member> it = getSecondaries().iterator(); it.hasNext(); ) {
        Member m = it.next();
        b.append("Secondary " + Prop.NL + m + Prop.NL);
      }

      return b.toString();
    }
  }

  public class Start implements Serializable {
    /** Address for the server to bind to for clients to connect. */
    private final InetSocketAddress tcpAddr;
    /** Connect address for remote akka system. {@code null}, if no connect. */
    private final Address remote;

    public Start(final InetSocketAddress tcpSocket, Address remote) {
      this.tcpAddr = tcpSocket;
      this.remote = remote;
    }

    public InetSocketAddress getTcpAddr() {
      return tcpAddr;
    }

    public Address getRemoteAddr() {
      return remote;
    }

    public ActorPath getRemotePath() {
      return new RootActorPath(getRemoteAddr(), "/user/replication");
    }
  }

  public class RequestStatus implements Serializable {}

  /**
   * Start the authentication process with a client.
   */
  public class StartAuthentication implements Serializable {
    /** TCP channel. */
    private final ActorRef channel;

    public StartAuthentication(ActorRef channel) {
      this.channel = channel;
    }

    public ActorRef getChannel() {
      return channel;
    }
  }
}
