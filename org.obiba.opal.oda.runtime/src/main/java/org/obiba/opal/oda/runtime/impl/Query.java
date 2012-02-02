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
import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSet;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.SortSpec;
import org.eclipse.datatools.connectivity.oda.spec.QuerySpecification;
import org.obiba.opal.rest.client.magma.UriBuilder;
import org.obiba.opal.web.model.Magma.ValueSetDto;
import org.obiba.opal.web.model.Magma.ValueSetsDto;
import org.obiba.opal.web.model.Magma.VariableDto;
import org.obiba.opal.web.model.Magma.VariableEntityDto;

/**
 * Implementation class of IQuery for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 */
public class Query implements IQuery {

  public static final String DATASOURCE = "DATASOURCE";

  public static final String TABLE = "TABLE";

  public static final String SELECT = "SELECT";

  private int maxRows;

  private String preparedText;

  private Connection connection;

  private Map<String, String> properties = new HashMap<String, String>();

  /**
   * @param connection
   */
  public Query(Connection connection) {
    super();
    this.connection = connection;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#prepare(java.lang.String)
   */
  public void prepare(String queryText) throws OdaException {
    preparedText = queryText;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setAppContext(java.lang.Object)
   */
  public void setAppContext(Object context) throws OdaException {
    // do nothing; assumes no support for pass-through context
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#close()
   */
  public void close() throws OdaException {
    // TODO Auto-generated method stub
    preparedText = null;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#getMetaData()
   */
  public IResultSetMetaData getMetaData() throws OdaException {
    URI uri = addNonNullQuery(fromBase("variables"), "script", getSelect()).build();
    List<VariableDto> variables = connection.getOpal().getResources(VariableDto.class, uri, VariableDto.newBuilder());
    return new ResultSetMetaData(variables);
  }

  public ValueSetDto getValueSet(String identifier) throws OdaException {
    URI uri = addNonNullQuery(fromBase("valueSet", identifier), "script", getSelect()).build();
    return connection.getOpal().getResource(ValueSetDto.class, uri, ValueSetDto.newBuilder());
  }

  public ValueSetsDto getValueSets(Integer offset, Integer limit) throws OdaException {
    URI uri = addNonNullQuery(//
    fromBase("valueSets"),//
    "select", getSelect(),//
    "offset", (offset == null ? null : offset.toString()),//
    "limit", (limit == null ? null : limit.toString())).build();
    return connection.getOpal().getResource(ValueSetsDto.class, uri, ValueSetsDto.newBuilder());
  }

  public List<VariableEntityDto> getEntities() throws OdaException {
    URI uri = addNonNullQuery(fromBase("entities")).build();
    return connection.getOpal().getResources(VariableEntityDto.class, uri, VariableEntityDto.newBuilder());
  }

  private UriBuilder addNonNullQuery(UriBuilder builder, String... params) {
    for(int i = 0; i < params.length; i += 2) {
      addNonNullQuery(builder, params[i], params[i + 1]);
    }
    return builder;
  }

  private UriBuilder addNonNullQuery(UriBuilder builder, String param, String value) {
    if(value != null && value.length() > 0) {
      builder.query(param, value);
    }
    return builder;
  }

  private UriBuilder fromBase(String... segments) {
    return baseUri().segment(segments);
  }

  private UriBuilder baseUri() {
    return connection.getOpal().newUri().segment("datasource", getDatasource(), "table", getTable());
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#executeQuery()
   */
  public IResultSet executeQuery() throws OdaException {
    IResultSet resultSet = new ResultSet(this);
    resultSet.setMaxRows(getMaxRows());
    return resultSet;
  }

  public String getDatasource() {
    return properties.get(DATASOURCE);
  }

  public String getTable() {
    return properties.get(TABLE);
  }

  public String getSelect() {
    return properties.get(SELECT);
  }

  public Connection getConnection() {
    return connection;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setProperty(java.lang.String, java.lang.String)
   */
  public void setProperty(String name, String value) throws OdaException {
    properties.put(name, value);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setMaxRows(int)
   */
  public void setMaxRows(int max) throws OdaException {
    maxRows = max;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#getMaxRows()
   */
  public int getMaxRows() throws OdaException {
    return maxRows;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#clearInParameters()
   */
  public void clearInParameters() throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(java.lang.String, int)
   */
  public void setInt(String parameterName, int value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setInt(int, int)
   */
  public void setInt(int parameterId, int value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(java.lang.String, double)
   */
  public void setDouble(String parameterName, double value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setDouble(int, double)
   */
  public void setDouble(int parameterId, double value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(java.lang.String, java.math.BigDecimal)
   */
  public void setBigDecimal(String parameterName, BigDecimal value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setBigDecimal(int, java.math.BigDecimal)
   */
  public void setBigDecimal(int parameterId, BigDecimal value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(java.lang.String, java.lang.String)
   */
  public void setString(String parameterName, String value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setString(int, java.lang.String)
   */
  public void setString(int parameterId, String value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(java.lang.String, java.sql.Date)
   */
  public void setDate(String parameterName, Date value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setDate(int, java.sql.Date)
   */
  public void setDate(int parameterId, Date value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(java.lang.String, java.sql.Time)
   */
  public void setTime(String parameterName, Time value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setTime(int, java.sql.Time)
   */
  public void setTime(int parameterId, Time value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(java.lang.String, java.sql.Timestamp)
   */
  public void setTimestamp(String parameterName, Timestamp value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setTimestamp(int, java.sql.Timestamp)
   */
  public void setTimestamp(int parameterId, Timestamp value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(java.lang.String, boolean)
   */
  public void setBoolean(String parameterName, boolean value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setBoolean(int, boolean)
   */
  public void setBoolean(int parameterId, boolean value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(java.lang.String, java.lang.Object)
   */
  public void setObject(String parameterName, Object value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setObject(int, java.lang.Object)
   */
  public void setObject(int parameterId, Object value) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(java.lang.String)
   */
  public void setNull(String parameterName) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setNull(int)
   */
  public void setNull(int parameterId) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to input parameter
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#findInParameter(java.lang.String)
   */
  public int findInParameter(String parameterName) throws OdaException {
    // TODO Auto-generated method stub
    // only applies to named input parameter
    return 0;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#getParameterMetaData()
   */
  public IParameterMetaData getParameterMetaData() throws OdaException {
    // return new ParameterMetaData();
    return null;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#setSortSpec(org.eclipse.datatools.connectivity.oda.SortSpec)
   */
  public void setSortSpec(SortSpec sortBy) throws OdaException {
    // only applies to sorting, assumes not supported
    throw new UnsupportedOperationException();
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IQuery#getSortSpec()
   */
  public SortSpec getSortSpec() throws OdaException {
    // only applies to sorting
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @seeorg.eclipse.datatools.connectivity.oda.IQuery#setSpecification(org.eclipse.datatools.connectivity.oda.spec.
   * QuerySpecification)
   */
  @SuppressWarnings("restriction")
  public void setSpecification(QuerySpecification querySpec) throws OdaException, UnsupportedOperationException {
    // assumes no support
    throw new UnsupportedOperationException();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#getSpecification()
   */
  @SuppressWarnings("restriction")
  public QuerySpecification getSpecification() {
    // assumes no support
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#getEffectiveQueryText()
   */
  public String getEffectiveQueryText() {
    // TODO Auto-generated method stub
    return preparedText;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.IQuery#cancel()
   */
  public void cancel() throws OdaException, UnsupportedOperationException {
    // assumes unable to cancel while executing a query
    throw new UnsupportedOperationException();
  }

}
