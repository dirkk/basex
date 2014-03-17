package org.basex.server.replication;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;
import org.basex.util.Prop;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

import static org.basex.server.replication.ReplicaSet.ReplicaSetState;

/**
 * Replica set messages.
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
     * Replica set status message
     * @param state state of the cluster
     * @param primary primary, if any
     * @param secondaries all secondaries
     */
    public StatusMessage(ReplicaSetState state, Member primary, List<Member> secondaries) {
      this.state = state;
      this.primary = primary;
      this.secondaries = secondaries;
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
    /** Cluster addres to connect to. */
    private final Address connectAddr;

    public Start(InetSocketAddress tcpSocket, Address connectAddr) {
      this.tcpAddr = tcpSocket;
      this.connectAddr = connectAddr;
    }

    public InetSocketAddress getTcpAddr() {
      return tcpAddr;
    }

    public Address getConnectAddr() {
      return connectAddr;
    }

    public ActorPath getConnectPath() {
      return new RootActorPath(getConnectAddr(), "/user/replication");
    }
  }

  public class RequestStatus implements Serializable {}
}
