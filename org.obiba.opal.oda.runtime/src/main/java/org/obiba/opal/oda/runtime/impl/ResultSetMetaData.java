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

import java.util.List;

import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.obiba.opal.web.model.Magma.AttributeDto;
import org.obiba.opal.web.model.Magma.VariableDto;

/**
 * Implementation class of IResultSetMetaData for an ODA runtime driver. <br>
 * For demo purpose, the auto-generated method stubs have hard-coded implementation that returns a pre-defined set of
 * meta-data and query results. A custom ODA driver is expected to implement own data source specific behavior in its
 * place.
 */
public class ResultSetMetaData implements IResultSetMetaData {

  public static final String ENTITY_IDENTIFIER = "ID";

  private List<VariableDto> variables;

  private String entityType;

  public ResultSetMetaData(List<VariableDto> variables) {
    super();
    this.variables = variables;
  }

  /**
   * Get variable at 1-based column index.
   * @param index
   * @return
   * @throws OdaException
   */
  private VariableDto getVariable(int index) throws OdaException {
    return variables.get(index - 2);
  }

  public String getEntityType() {
    if(entityType == null) {
      try {
        entityType = variables.get(0).getEntityType();
      } catch(Exception e) {
        entityType = "Participant";
      }
    }
    return entityType;
  }

  public String getEntityIdentifierColumnName() {
    return getEntityType() + " ID";
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnCount()
   */
  public int getColumnCount() throws OdaException {
    return variables.size() + 1;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnName(int)
   */
  public String getColumnName(int index) throws OdaException {
    if(index == 1) return getEntityIdentifierColumnName();
    return getVariable(index).getName();
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnLabel(int)
   */
  public String getColumnLabel(int index) throws OdaException {
    if(index == 1) return getEntityIdentifierColumnName();

    String label = null;
    try {
      VariableDto variable = getVariable(index);
      if(variable.getAttributesCount() > 0) {
        for(AttributeDto attribute : variable.getAttributesList()) {
          if(attribute.getName().equals("alias")) {
            label = attribute.getValue();
            break;
          }
        }

      }
    } finally {
      if(label == null || label.trim().length() == 0) {
        label = getColumnName(index); // default
      }
    }
    return label;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnType(int)
   */
  public int getColumnType(int index) throws OdaException {
    if(index == 1) return java.sql.Types.VARCHAR;

    String valueType = getVariable(index).getValueType();
    if(valueType.equals("text")) {
      return java.sql.Types.VARCHAR;
    } else if(valueType.equals("integer")) {
      return java.sql.Types.INTEGER;
    } else if(valueType.equals("decimal")) {
      return java.sql.Types.DECIMAL;
    } else if(valueType.equals("boolean")) {
      return java.sql.Types.BOOLEAN;
    } else if(valueType.equals("date")) {
      return java.sql.Types.DATE;
    } else if(valueType.equals("datetime")) {
      return java.sql.Types.TIMESTAMP;
    } else if(valueType.equals("binary")) {
      return java.sql.Types.BINARY;
    } else if(valueType.equals("locale")) {
      return java.sql.Types.VARCHAR;
    } else {
      throw new OdaException("Unidentified variable value type: " + valueType);
    }
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnTypeName(int)
   */
  public String getColumnTypeName(int index) throws OdaException {
    int nativeTypeCode = getColumnType(index);
    return Driver.getNativeDataTypeName(nativeTypeCode);
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getColumnDisplayLength(int)
   */
  public int getColumnDisplayLength(int index) throws OdaException {
    // TODO replace with data source specific implementation

    // hard-coded for demo purpose
    return 8;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getPrecision(int)
   */
  public int getPrecision(int index) throws OdaException {
    // TODO Auto-generated method stub
    return -1;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#getScale(int)
   */
  public int getScale(int index) throws OdaException {
    // TODO Auto-generated method stub
    return -1;
  }

  /*
   * @see org.eclipse.datatools.connectivity.oda.IResultSetMetaData#isNullable(int)
   */
  public int isNullable(int index) throws OdaException {
    // TODO Auto-generated method stub
    return IResultSetMetaData.columnNullableUnknown;
  }

}
