/*******************************************************************************
 * Copyright 2008(c) The OBiBa Consortium. All rights reserved.
 * 
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package org.obiba.opal.oda.runtime.impl;

import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Logger;

import org.eclipse.datatools.connectivity.oda.IConnection;
import org.eclipse.datatools.connectivity.oda.IDataSetMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.obiba.opal.rest.client.magma.OpalJavaClient;

import com.ibm.icu.util.ULocale;

/**
 * Implementation class of IConnection for an ODA runtime driver.
 */
public class Connection implements IConnection {

  private boolean m_isOpen = false;

  private String wsURL;

  private String username;

  private String password;

  private OpalJavaClient opal;

  private Logger log = Logger.getLogger(Connection.class.getName());

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#open(java.util.Properties)
   */
  public void open(Properties connProperties) throws OdaException {
    // Check for null properties
    if(connProperties == null) throw new OdaException("No Connection Properties Provided.");

    // Set global path, username, and password
    wsURL = connProperties.getProperty("URL");
    username = connProperties.getProperty("USER");
    password = connProperties.getProperty("PASSWORD");

    // Make sure there is a slash at the end of the path
    if(!wsURL.endsWith("/")) wsURL += "/";
    wsURL += "ws/";

    try {
      this.opal = new OpalJavaClient(wsURL, username, password);
    } catch(URISyntaxException e) {
      throw new OdaException(e);
    }

    m_isOpen = true;
  }

  public OpalJavaClient getOpal() {
    return opal;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#setAppContext(java.lang.Object)
   */
  public void setAppContext(Object context) throws OdaException {
    // do nothing; assumes no support for pass-through context
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#close()
   */
  public void close() throws OdaException {
    // TODO check for session timeout
    if(opal != null) {
    }

    m_isOpen = false;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#isOpen()
   */
  public boolean isOpen() throws OdaException {
    return m_isOpen;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#getMetaData(java.lang.String)
   */
  public IDataSetMetaData getMetaData(String dataSetType) throws OdaException {
    // assumes that this driver supports only one type of data set,
    // ignores the specified dataSetType
    return new DataSetMetaData(this);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#newQuery(java.lang.String)
   */
  public IQuery newQuery(String dataSetType) throws OdaException {
    // assumes that this driver supports only one type of data set,
    // ignores the specified dataSetType
    return new Query(this);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#getMaxQueries()
   */
  public int getMaxQueries() throws OdaException {
    return 0; // no limit
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#commit()
   */
  public void commit() throws OdaException {
    // do nothing; assumes no transaction support needed
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IConnection#rollback()
   */
  public void rollback() throws OdaException {
    // do nothing; assumes no transaction support needed
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IConnection#setLocale(com.ibm.icu.util.ULocale)
   */
  public void setLocale(ULocale locale) throws OdaException {
    // do nothing; assumes no locale support
  }

}
