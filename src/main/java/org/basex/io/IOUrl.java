package org.basex.io;

import static org.basex.core.Text.*;

import java.io.*;
import java.net.*;
import java.util.regex.*;

import org.basex.core.*;
import org.basex.io.in.*;
import org.basex.util.*;
import org.xml.sax.*;

/**
 * {@link IO} reference, representing a URL.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class IOUrl extends IO {
  /**
   * Constructor.
   * @param u url
   */
  public IOUrl(final String u) {
    super(u);
  }

  @Override
  public byte[] read() throws IOException {
    return new BufferInput(this).content();
  }

  @Override
  public InputSource inputSource() {
    return new InputSource(path);
  }

  @Override
  public InputStream inputStream() throws IOException {
    final URL url = new URL(path);
    try {
      return url.openStream();
    } catch(final IOException ex) {
      final IOException io = new IOException(Util.message(ex));
      io.setStackTrace(ex.getStackTrace());
      throw io;
    } catch(final RuntimeException ex) {
      // catch unexpected runtime exceptions
      Util.debug(ex);
      throw new BaseXException(NOT_PARSED_X, path);
    }
  }

  @Override
  public long length() {
    return 0;
  }

  @Override
  public IO merge(final String f) {
    final IO io = IO.get(f);
    if(!(io instanceof IOFile) || f.contains(":") || f.startsWith("/")) return io;
    return IO.get((path.endsWith("/") ? path : path.replace("^(.*/).*", "$1")) + f);
  }

  /** Pattern for duplicate slashes. */
  private static final Pattern DUPLSLASH = Pattern.compile("//+");
  /** Pattern for leading slash in Windows paths. */
  private static final Pattern LEADSLASH = Pattern.compile("^/([A-Za-z]:)");

  /**
   * Creates a file path from the specified URL.
   * @param url url to be converted
   * @return file path
   */
  public static String file(final String url) {
    String file = url;
    try {
      if(file.indexOf('%') != -1) file = URLDecoder.decode(file, Prop.ENCODING);
    } catch(final Exception ex) { /* ignored. */ }
    // remove file scheme
    if(file.startsWith(FILEPREF)) file = file.substring(FILEPREF.length());
    // remove duplicate slashes and leading slash in Windows paths
    if(file.contains("//")) file = DUPLSLASH.matcher(file).replaceAll("/");
    return LEADSLASH.matcher(file).replaceFirst("$1");
  }
}
