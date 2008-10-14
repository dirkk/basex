package org.basex.api.xmldb;

import java.io.IOException;
import org.w3c.dom.Document;
import org.xmldb.api.base.*;
import org.xmldb.api.modules.XMLResource;
import org.basex.BaseX;
import org.basex.build.MemBuilder;
import org.basex.build.Parser;
import org.basex.build.xml.DOCWrapper;
import org.basex.build.xml.DirParser;
import org.basex.core.Context;
import org.basex.core.proc.Close;
import org.basex.data.Data;
import org.basex.io.IO;
import org.basex.util.StringList;
import org.basex.util.Token;

/**
 * Implementation of the Collection Interface for the XMLDB:API.
 * @author Workgroup DBIS, University of Konstanz 2005-08, ISC License
 * @author Andreas Weiler
 */
public class BXCollection implements Collection {
  /** Context reference. */
  Context ctx;
  /** Boolean value if Collection is closed. */
  boolean closed;

  /**
   * Standard constructor.
   * @param c for Context
   */
  public BXCollection(final Context c) {
    ctx = c;
    closed = false;
  }

  public void close() {
    new Close().execute(ctx);
    closed = true;
  }

  public String createId() {
    return String.valueOf(ctx.data().size + 1);
  }

  public Resource createResource(final String id, final String type)
      throws XMLDBException {
    if(isOpen()) {
      if(type.equals(XMLResource.RESOURCE_TYPE)) {
        return new BXXMLResource(null, id, -1, this);
      }
      throw new XMLDBException(ErrorCodes.UNKNOWN_RESOURCE_TYPE);
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public Collection getChildCollection(final String name) throws XMLDBException {
    if(isOpen()) {
      return null;
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public int getChildCollectionCount() throws XMLDBException {
    if(isOpen()) {
      return 0;
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public String getName() {
    return ctx.data().meta.dbname;
  }

  public Collection getParentCollection() throws XMLDBException {
    if(isOpen()) {
      return null;
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public String getProperty(final String name) {
    //<CG> Was für Properties gibt es?
    BaseX.notimplemented();
    return null;
  }

  public Resource getResource(final String id) throws XMLDBException {
    if(isOpen()) {
      final byte[] idd = Token.token(id);
      for(final int d : ctx.data().doc()) {
        if(Token.eq(ctx.data().text(d), idd)) {
          return new BXXMLResource(ctx.data(), id, d, this);
        }
      }
      return null;
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public int getResourceCount() throws XMLDBException {
    if(isOpen()) {
      return ctx.data().doc().length;
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public Service getService(final String name, final String version)
      throws XMLDBException {
    if(isOpen()) {
      if(name.equals(BXQueryService.XPATH)
          || name.equals(BXQueryService.XQUERY)) return new BXQueryService(
          this, name);
      if(name.equals(BXCollectionManagementService.MANAGEMENT)) return new
      BXCollectionManagementService(this);
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public Service[] getServices() throws XMLDBException {
    if(isOpen()) {
      return new Service[] { getService(BXQueryService.XPATH, null),
          getService(BXQueryService.XQUERY, null), getService(BXCollectionManagementService.MANAGEMENT, null) };
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public boolean isOpen() {
    return !closed;
  }

  public String[] listChildCollections() throws XMLDBException {
    if(isOpen()) {
      String[] empty = {};
      return empty;
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public String[] listResources() throws XMLDBException {
    if(isOpen()) {
      final StringList sl = new StringList();
      for(int d : ctx.data().doc()) sl.add(Token.string(ctx.data().text(d)));
      return sl.finish();
    }
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
  }

  public void removeResource(Resource res) throws XMLDBException {
    if(isOpen()) {
      if(res instanceof BXXMLResource) {
        BXXMLResource tmp = (BXXMLResource) res;
        if(Token.string(ctx.data().text(tmp.getPre())).equals(
            tmp.getDocumentId())) {
          ctx.data().delete(((BXXMLResource) res).getPre());
          ctx.data().flush();
        } else {
        throw new XMLDBException(ErrorCodes.NO_SUCH_RESOURCE);
        }
       } else {
      throw new XMLDBException(ErrorCodes.INVALID_RESOURCE);
      }
    } else {
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
    }
  }

  public void setProperty(final String name, final String value) {
    //<CG> Was für Properties gibt es?
    BaseX.notimplemented();
  }

  public void storeResource(final Resource res) throws XMLDBException {
    if(isOpen()) {
      final String id = ((BXXMLResource) res).getDocumentId();
      Data tmp = null;
      final Object cont = res.getContent();
      Parser p = null;
      if(cont instanceof Document) {
        p = new DOCWrapper((Document) cont, id);
      } else {
        p = new DirParser(IO.get(cont.toString()));
      }
      try {
        tmp = new MemBuilder().build(p, id);
        final Data data = ctx.data();
        data.insert(data.size, -1, tmp);
        data.flush();
      } catch(final IOException ex) {
        BaseX.debug(ex);
        throw new XMLDBException(ErrorCodes.INVALID_RESOURCE);
      }
    } else {
    throw new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
    }
  }
}
