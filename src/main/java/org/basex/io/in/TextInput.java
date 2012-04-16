package org.basex.io.in;

import static org.basex.util.Token.*;

import java.io.*;

import org.basex.io.*;
import org.basex.util.*;
import org.basex.util.list.*;

/**
 * This class provides a convenient access to text input.
 * The input encoding will be guessed by analyzing the first bytes.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public class TextInput extends BufferInput {
  /** Encoding. */
  private String encoding;
  /** Decoder. */
  private TextDecoder decoder;

  /**
   * Constructor.
   * @param is input stream
   * @throws IOException I/O exception
   */
  public TextInput(final InputStream is) throws IOException {
    super(is);
    guess();
  }

  /**
   * Constructor.
   * @param io input
   * @throws IOException I/O exception
   */
  public TextInput(final IO io) throws IOException {
    super(io);
    guess();
  }

  /**
   * Tries to guess the character encoding, based on the first bytes.
   * @throws IOException I/O exception
   */
  private void guess() throws IOException {
    final int a = next();
    final int b = next();
    final int c = next();
    final int d = next();
    String e = null;
    int skip = 0;
    if(a == 0xFF && b == 0xFE) { // BOM: FF FE
      e = UTF16LE;
      skip = 2;
    } else if(a == 0xFE && b == 0xFF) { // BOM: FE FF
      e = UTF16BE;
      skip = 2;
    } else if(a == 0xEF && b == 0xBB && c == 0xBF) { // BOM: EF BB BF
      skip = 3;
    } else if(a == '<' && b == 0 && c == '?' && d == 0) {
      e = UTF16LE;
    } else if(a == 0 && b == '<' && c == 0 && d == '?') {
      e = UTF16BE;
    }
    reset();
    for(int s = 0; s < skip; s++) next();
    encoding(e);
  }

  /**
   * Returns the encoding.
   * Sets a new encoding.
   * @return encoding
   */
  public String encoding() {
    return encoding;
  }

  /**
   * Explicitly sets a new encoding.
   * @param enc encoding (ignored if set to {@code null})
   * @return self reference
   * @throws IOException I/O Exception
   */
  public TextInput encoding(final String enc) throws IOException {
    encoding = normEncoding(enc, encoding);
    decoder = TextDecoder.get(encoding);
    return this;
  }

  @Override
  public int read() throws IOException {
    return decoder.read(this);
  }

  @Override
  public byte[] content() throws IOException {
    final TokenBuilder tb = new TokenBuilder(Math.max(ElementList.CAP, (int) length));
    try {
      for(int ch; (ch = read()) != -1;) tb.add(ch);
    } finally {
      close();
    }
    return tb.finish();
  }
}
