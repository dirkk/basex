package org.basex.server.replication;

import static org.basex.data.DataText.*;
import static org.basex.util.Token.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.basex.core.*;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.io.in.*;
import org.basex.io.out.*;

/**
 * A document holding message. This class is send from a master to all
 * connected slaves, if the document has been updated. The document is
 * serialized as UTF-8.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Dirk Kirsten
 */
public class DatabaseMessage extends Message {
  /** Database instance. */
  private final Data data;
  /** Document content. */
  private byte[] content;
  /** Type value. */
  public static byte TYPE = 0x01;
  
  /**
   * Default constructor.
   *
   * @param d data structure
   */
  public DatabaseMessage(final Data d) {
    data = d;
  }

  /**
   * Constructor.
   * Create a document message object from a incoming byte array.
   * 
   * @param bi block input
   */
  public DatabaseMessage(final BlockInput bi) {
    data = null;
    
    // meta information
    MetaData meta = new MetaData(new Prop());
    readMetadata(bi, meta);
    
    // content
    content = bi.readBlock();
  }
  
  /**
   * Read in all the meta-data information of a database, given in the message.
   * 
   * @param in input data
   * @param meta meta-data to update
   */
  public void readMetadata(final BlockInput in, final MetaData meta) {
    byte amount = in.readByte();
    for (byte i = 0; i < amount; ++i) {
      String k = in.readString();
      String v = in.readString();
      
      if(k.equals(DBFNAME))         meta.original   = v;
      else if(k.equals(DBTIME))     meta.time       = toLong(v);
      else if(k.equals(DBFSIZE))    meta.filesize   = toLong(v);
      else if(k.equals(DBNDOCS))    meta.ndocs      = toInt(v);
      else if(k.equals(DBENC))      meta.encoding   = v;
      else if(k.equals(DBSIZE))     meta.size       = toInt(v);
      else if(k.equals(DBCHOP))     meta.chop       = new Boolean(v);
      else if(k.equals(DBUPDIDX))   meta.updindex   = new Boolean(v);
      else if(k.equals(DBTXTIDX))   meta.textindex  = new Boolean(v);
      else if(k.equals(DBATVIDX))   meta.attrindex  = new Boolean(v);
      else if(k.equals(DBFTXIDX))   meta.ftxtindex  = new Boolean(v);
      else if(k.equals(DBCRTTXT))   meta.createtext = new Boolean(v);
      else if(k.equals(DBCRTATV))   meta.createattr = new Boolean(v);
      else if(k.equals(DBCRTFTX))   meta.createftxt = new Boolean(v);
      else if(k.equals(DBFTST))     meta.stemming   = new Boolean(v);
      else if(k.equals(DBFTCS))     meta.casesens   = new Boolean(v);
      else if(k.equals(DBFTDC))     meta.diacritics = new Boolean(v);
      else if(k.equals(DBFTSW))     meta.stopwords  = v;
      else if(k.equals(DBMAXLEN))   meta.maxlen     = toInt(v);
      else if(k.equals(DBMAXCATS))  meta.maxcats    = toInt(v);
      else if(k.equals(DBUPTODATE)) meta.uptodate   = new Boolean(v);
      else if(k.equals(DBLASTID))   meta.lastid     = toInt(v);
    }
  }

  /**
   * Writes the meta data to the specified output stream.
   * @param out output stream
   */
  public void writeMetadata(final BlockOutput out) {
    MetaData meta = data.meta;
    
    out.writeByte((byte) 22);
    out.writePair(DBFNAME,    meta.original);
    out.writePair(DBTIME,     Long.toString(meta.time));
    out.writePair(DBFSIZE,    Long.toString(meta.filesize));
    out.writePair(DBNDOCS,    Long.toString(meta.ndocs));
    out.writePair(DBENC,      meta.encoding);
    out.writePair(DBSIZE,     Long.toString(meta.size));
    out.writePair(DBCHOP,     Boolean.toString(meta.chop));
    out.writePair(DBUPDIDX,   Boolean.toString(meta.updindex));
    out.writePair(DBTXTIDX,   Boolean.toString(meta.textindex));
    out.writePair(DBATVIDX,   Boolean.toString(meta.attrindex));
    out.writePair(DBFTXIDX,   Boolean.toString(meta.ftxtindex));
    out.writePair(DBCRTTXT,   Boolean.toString(meta.createtext));
    out.writePair(DBCRTATV,   Boolean.toString(meta.createattr));
    out.writePair(DBCRTFTX,   Boolean.toString(meta.createftxt));
    out.writePair(DBFTST,     Boolean.toString(meta.stemming));
    out.writePair(DBFTCS,     Boolean.toString(meta.casesens));
    out.writePair(DBFTDC,     Boolean.toString(meta.diacritics));
    out.writePair(DBFTSW,     meta.stopwords);
    out.writePair(DBMAXLEN,   Long.toString(meta.maxlen));
    out.writePair(DBMAXCATS,  Long.toString(meta.maxcats));
    out.writePair(DBUPTODATE, Boolean.toString(meta.uptodate));
    out.writePair(DBLASTID,   Long.toString(meta.lastid));
  }
  
  /**
   * Serializes a complete database. It will encode the message
   * using prepending length fields. It send the whole base URI (include
   * database and file name) and the UTF-8 serialized document.
   * 
   * @return byte array holding the message.
   */
  @Override
  public byte[] serialize() {
    try {
      BlockOutput bo = new BlockOutput();
      
      // write the type
      bo.writeByte(DatabaseMessage.TYPE);
      // write metadata as pairs of key and value
      writeMetadata(bo);

      IOFile f = data.meta.dbfile(DATATXT);
      ArrayOutput fileOut = new ArrayOutput();
      // optimize buffer size
      final int bsize = (int) Math.max(1, Math.min(f.length(), 1 << 22));
      final byte[] buf = new byte[bsize];
      final BufferInput fis = new BufferInput(new IOFile(f.file()));
      for(int i; (i = fis.read(buf)) != -1;) fileOut.write(buf, 0, i);
      bo.writeBlock(fileOut.toArray());
      
      return bo.toArray();
    } catch(IOException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Get the file content as UTF-8 encoded string.
   * @return file content
   */
  public String getContent() {
    try {
      return new String(content, "UTF-8");
    } catch(UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * Save the whole database within the given context.
   * 
   * @param context database context
   * @throws BaseXException exception
   */
  @Override
  public void save(final Context context) throws BaseXException {
  }
}
