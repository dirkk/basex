package org.basex.server.replication;

import org.basex.BaseXMember;
import org.basex.util.list.StringList;
import org.junit.Test;

import java.io.IOException;

/**
 * @author BaseX Team 2005-14, BSD License
 * @author Dirk Kirsten
 */
public class BaseXMemberTest {
  @Test
  public void start() throws IOException {
    StringList sl = new StringList().add("-z").add("-p9999");
    BaseXMember m = new BaseXMember(sl.toArray());
    m.stop();
  }

  @Test
  public void connect() throws IOException {
    StringList sl1 = new StringList().add("-z").add("-p9999");
    BaseXMember m1 = new BaseXMember(sl1.toArray());

    StringList sl2 = new StringList().add("-z").add("-p8999").add("-x127.0.0.1").add("-y10000");
    BaseXMember m2 = new BaseXMember(sl2.toArray());

    m1.stop();
    m2.stop();
  }
}
