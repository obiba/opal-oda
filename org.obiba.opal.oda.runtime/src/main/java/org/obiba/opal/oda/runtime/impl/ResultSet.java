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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.eclipse.datatools.connectivity.oda.IBlob;
import org.eclipse.datatools.connectivity.oda.IClob;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.obiba.opal.web.model.Magma.ValueSetsDto;
import org.obiba.opal.web.model.Magma.VariableEntityDto;

/**
 * Implementation class of IResultSet for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 */
public class ResultSet implements IResultSet {

  private static final int VALUE_SET_BUFFER_LENGTH = 100;

  private final SimpleDateFormat DATETIME_ISO_8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");

  private int maxRows;

  private int currentRowId;

  private ValueSetsDto.ValueSetDto currentValueSet;

  private Query query;

  private ResultSetMetaData metaData;

  private List<VariableEntityDto> entities;

  private ValueSetsDto valueSetBuffer;

  private int valueSetOffset;

  private boolean wasNull;

  private int[] columnToVariableIndices;

  /**
   * @param connection
   */
  public ResultSet(Query query) {
    super();
    this.query = query;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getMetaData()
   */
  public IResultSetMetaData getMetaData() throws OdaException {
    if(metaData == null) {
      metaData = (ResultSetMetaData) query.getMetaData();
    }
    return metaData;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#setMaxRows(int)
   */
  public void setMaxRows(int max) throws OdaException {
    maxRows = Math.min(max, getEntities().size());
  }

  /**
   * Returns the maximum number of rows that can be fetched from this result set.
   * @return the maximum number of rows to fetch.
   */
  protected int getMaxRows() {
    return maxRows;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#next()
   */
  public boolean next() throws OdaException {
    if((maxRows == 0 || currentRowId < maxRows) && currentRowId < getEntities().size()) {
      currentValueSet = getValueSetAt(currentRowId);
      currentRowId++;
      return true;
    }

    return false;
  }

  private List<VariableEntityDto> getEntities() throws OdaException {
    if(entities == null) {
      entities = query.getEntities();
    }
    return entities;
  }

  private ValueSetsDto.ValueSetDto getValueSetAt(int index) throws OdaException {
    if(valueSetBuffer == null) {
      valueSetOffset = 0;
      valueSetBuffer = query.getValueSets(0, VALUE_SET_BUFFER_LENGTH);
    } else if(index >= valueSetOffset + VALUE_SET_BUFFER_LENGTH) {
      valueSetOffset += VALUE_SET_BUFFER_LENGTH;
      valueSetBuffer = query.getValueSets(valueSetOffset, VALUE_SET_BUFFER_LENGTH);
    }
    return valueSetBuffer.getValueSets(index - valueSetOffset);
  }

  /**
   * Get the string value at the 1-based column index for the current row.
   * @param index
   * @return
   */
  private String getValueAt(int index) throws OdaException {
    // index is the column number, but the variables are not in the same order in the value set
    // so build a map from column index to variable index
    if(columnToVariableIndices == null) {
      columnToVariableIndices = new int[getMetaData().getColumnCount()];
      for(int i = 2; i <= getMetaData().getColumnCount(); i++) {
        String columnName = getMetaData().getColumnName(i);
        columnToVariableIndices[i - 1] = findValueSetColumn(columnName);
        // log.info(columnName + " [" + (i - 1) + "]=" + columnToVariableIndices[i - 1]);
      }
    }

    String rval = null;
    if(index == 1) {
      rval = currentValueSet.getIdentifier();
    } else {
      ValueSetsDto.ValueDto value = currentValueSet.getValues(columnToVariableIndices[index - 1]);
      if(value.hasValue()) {
        rval = value.getValue();
      }
    }
    wasNull = (rval == null);
    return rval;

  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#close()
   */
  public void close() throws OdaException {
    entities = null;
    currentRowId = 0; // reset row counter
    valueSetBuffer = null;
    valueSetOffset = 0;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getRow()
   */
  public int getRow() throws OdaException {
    return currentRowId;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getString(int)
   */
  public String getString(int index) throws OdaException {
    return getValueAt(index);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getString(java.lang.String)
   */
  public String getString(String columnName) throws OdaException {
    return getString(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getInt(int)
   */
  public int getInt(int index) throws OdaException {
    String value = getValueAt(index);
    if(value == null) return 0;
    return Integer.parseInt(value);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getInt(java.lang.String)
   */
  public int getInt(String columnName) throws OdaException {
    return getInt(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(int)
   */
  public double getDouble(int index) throws OdaException {
    String value = getValueAt(index);
    if(value == null) return 0;
    return Double.parseDouble(value);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDouble(java.lang.String)
   */
  public double getDouble(String columnName) throws OdaException {
    return getDouble(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(int)
   */
  public BigDecimal getBigDecimal(int index) throws OdaException {
    String value = getValueAt(index);
    if(value == null) return null;
    return new BigDecimal(value);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBigDecimal(java.lang.String)
   */
  public BigDecimal getBigDecimal(String columnName) throws OdaException {
    return getBigDecimal(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDate(int)
   */
  public Date getDate(int index) throws OdaException {
    String value = getValueAt(index);
    if(value == null) return null;
    return Date.valueOf(value);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getDate(java.lang.String)
   */
  public Date getDate(String columnName) throws OdaException {
    return getDate(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTime(int)
   */
  public Time getTime(int index) throws OdaException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTime(java.lang.String)
   */
  public Time getTime(String columnName) throws OdaException {
    return getTime(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(int)
   */
  public Timestamp getTimestamp(int index) throws OdaException {
    String value = getValueAt(index);
    if(value == null) return null;
    try {
      return new Timestamp(DATETIME_ISO_8601.parse(value).getTime());
    } catch(ParseException e) {
      throw new OdaException(e);
    }
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getTimestamp(java.lang.String)
   */
  public Timestamp getTimestamp(String columnName) throws OdaException {
    return getTimestamp(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(int)
   */
  public IBlob getBlob(int index) throws OdaException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBlob(java.lang.String)
   */
  public IBlob getBlob(String columnName) throws OdaException {
    return getBlob(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getClob(int)
   */
  public IClob getClob(int index) throws OdaException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getClob(java.lang.String)
   */
  public IClob getClob(String columnName) throws OdaException {
    return getClob(findColumn(columnName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(int)
   */
  public boolean getBoolean(int index) throws OdaException {
    String value = getValueAt(index);
    if(value == null) return false;
    return Boolean.parseBoolean(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getBoolean(java.lang.String)
   */
  public boolean getBoolean(String columnName) throws OdaException {
    return getBoolean(findColumn(columnName));
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getObject(int)
   */
  public Object getObject(int index) throws OdaException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#getObject(java.lang.String)
   */
  public Object getObject(String columnName) throws OdaException {
    return getObject(findColumn(columnName));
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#wasNull()
   */
  public boolean wasNull() throws OdaException {
    return wasNull;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSet#findColumn(java.lang.String)
   */
  public int findColumn(String columnName) throws OdaException {
    for(int i = 1; i <= getMetaData().getColumnCount(); i++) {
      if(getMetaData().getColumnName(i).equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

  public int findValueSetColumn(String columnName) throws OdaException {
    List<String> variables = valueSetBuffer.getVariablesList();
    for(int i = 0; i < variables.size(); i++) {
      if(variables.get(i).equals(columnName)) {
        return i;
      }
    }
    return -1;
  }

}
