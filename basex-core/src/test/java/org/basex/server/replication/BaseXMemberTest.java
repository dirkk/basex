package org.basex.server.replication;

import org.basex.BaseXMember;
import org.basex.core.cmd.Info;
import org.basex.core.cmd.XQuery;
import org.basex.io.in.ArrayInput;
import org.basex.server.LoginException;
import org.basex.server.MemberSession;
import org.basex.util.list.StringList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class BaseXMemberTest {
  /** TCP port. */
  private final static int PORT = 9999;
  /** Akka port. */
  private final static int AKKAPORT = 5678;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void start() throws IOException {
    StringList sl = new StringList().add("-z").add("-p" + PORT).add("-a" + AKKAPORT);
    BaseXMember m = new BaseXMember(sl.toArray());
    m.stop();
  }

  @Test
  public void connect() throws IOException {
    StringList sl1 = new StringList().add("-z").add("-p" + PORT).add("-a" + AKKAPORT);
    BaseXMember m1 = new BaseXMember(sl1.toArray());

    StringList sl2 = new StringList().add("-z").add("-p" + (PORT + 100)).add("-a" + (AKKAPORT + 100)).add("-x127.0.0.1:" + AKKAPORT);
    BaseXMember m2 = new BaseXMember(sl2.toArray());

    m1.stop();
    m2.stop();
  }

  @Test
  public void clientConnect() throws IOException {
    StringList sl1 = new StringList().add("-z").add("-p" + PORT).add("-a" + AKKAPORT);
    System.setProperty("org.basex.DBPATH", "/tmp/BaseXMemberTest");
    BaseXMember m1 = new BaseXMember(sl1.toArray());

    MemberSession cs = new MemberSession("127.0.0.1", PORT, "admin", "admin", true);
    cs.execute(new Info());
    cs.create("test", new ArrayInput("<a><b/></a>"));
    String result = cs.execute(new XQuery("db:open('test')//a"));
    assert(result.equals("<a>\n  <b/>\n</a>"));

    m1.stop();
  }

  @Test
  public void clientAuthenticationFails() throws IOException {
    StringList sl1 = new StringList().add("-z").add("-p" + PORT).add("-a" + AKKAPORT);
    BaseXMember m1 = new BaseXMember(sl1.toArray());

    new MemberSession("127.0.0.1", PORT, "admin", "wrongpassword", true);
    thrown.expect(LoginException.class);

    m1.stop();
  }
}
