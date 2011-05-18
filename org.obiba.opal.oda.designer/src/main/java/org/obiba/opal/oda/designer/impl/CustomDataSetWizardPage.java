/*******************************************************************************
 * Copyright 2008(c) The OBiBa Consortium. All rights reserved.
 * 
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package org.obiba.opal.oda.designer.impl;

import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import org.eclipse.datatools.connectivity.oda.IParameterMetaData;
import org.eclipse.datatools.connectivity.oda.IQuery;
import org.eclipse.datatools.connectivity.oda.IResultSetMetaData;
import org.eclipse.datatools.connectivity.oda.OdaException;
import org.eclipse.datatools.connectivity.oda.design.DataSetDesign;
import org.eclipse.datatools.connectivity.oda.design.DataSetParameters;
import org.eclipse.datatools.connectivity.oda.design.DesignFactory;
import org.eclipse.datatools.connectivity.oda.design.ResultSetColumns;
import org.eclipse.datatools.connectivity.oda.design.ResultSetDefinition;
import org.eclipse.datatools.connectivity.oda.design.ui.designsession.DesignSessionUtil;
import org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage;
import org.eclipse.datatools.connectivity.oda.design.util.DesignUtil;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.obiba.opal.oda.runtime.impl.Connection;
import org.obiba.opal.oda.runtime.impl.Driver;
import org.obiba.opal.oda.runtime.impl.Query;
import org.obiba.opal.web.model.Magma.DatasourceDto;

/**
 * Auto-generated implementation of an ODA data set designer page for an user to create or edit an ODA data set design
 * instance. This custom page provides a simple Query Text control for user input. It further extends the DTP
 * design-time framework to update an ODA data set design instance based on the query's derived meta-data. <br>
 * A custom ODA designer is expected to change this exemplary implementation as appropriate.
 */
public class CustomDataSetWizardPage extends DataSetWizardPage {

  private Logger log = Logger.getLogger(CustomDataSetWizardPage.class.getName());

  private static String DEFAULT_MESSAGE = "Opal Table";

  private Connection connection;

  private transient Combo datasourceCombo;

  private transient Combo tableCombo;

  private List<DatasourceDto> datasources;

  private Text selectTextField;

  private Text whereTextField;

  private Text queryTextField;

  /**
   * Constructor
   * @param pageName
   */
  public CustomDataSetWizardPage(String pageName) {
    super(pageName);
    setTitle(pageName);
    setMessage(DEFAULT_MESSAGE);
  }

  /**
   * Constructor
   * @param pageName
   * @param title
   * @param titleImage
   */
  public CustomDataSetWizardPage(String pageName, String title, ImageDescriptor titleImage) {
    super(pageName, title, titleImage);
    setMessage(DEFAULT_MESSAGE);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#createPageCustomControl(org.eclipse.
   * swt.widgets.Composite)
   */
  public void createPageCustomControl(Composite parent) {
    setControl(createPageControl(parent));
    initializeControl();
  }

  /**
   * Creates custom control for user-defined query text.
   */
  private Control createPageControl(Composite parent) {
    Composite composite = new Composite(parent, SWT.NONE);
    composite.setLayout(new GridLayout(2, false));
    GridData gridData = new GridData(GridData.HORIZONTAL_ALIGN_FILL | GridData.VERTICAL_ALIGN_FILL);
    composite.setLayoutData(gridData);

    Label fieldLabel = new Label(composite, SWT.NONE);
    fieldLabel.setText("&Datasource:");

    datasourceCombo = new Combo(composite, SWT.DROP_DOWN);
    populateDatasourceCombo();
    datasourceCombo.select(0);
    datasourceCombo.addModifyListener(new ModifyListener() {

      @Override
      public void modifyText(ModifyEvent e) {
        updateTableCombo();
        updateQueryText();
      }
    });

    fieldLabel = new Label(composite, SWT.NONE);
    fieldLabel.setText("&Table:");

    tableCombo = new Combo(composite, SWT.DROP_DOWN);
    updateTableCombo();
    tableCombo.addModifyListener(new ModifyListener() {

      @Override
      public void modifyText(ModifyEvent e) {
        validateData();
        updateQueryText();
      }
    });

    fieldLabel = new Label(composite, SWT.NONE);
    fieldLabel.setText("&Variables Filter:");

    selectTextField = new Text(composite, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    GridData data = new GridData(GridData.FILL_HORIZONTAL);
    data.heightHint = 100;
    selectTextField.setLayoutData(data);
    selectTextField.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        validateData();
        updateQueryText();
      }
    });

    fieldLabel = new Label(composite, SWT.NONE);
    fieldLabel.setText("&Entities Filter:");

    whereTextField = new Text(composite, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    data = new GridData(GridData.FILL_HORIZONTAL);
    data.heightHint = 100;
    whereTextField.setLayoutData(data);
    whereTextField.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        validateData();
        updateQueryText();
      }
    });

    fieldLabel = new Label(composite, SWT.NONE);
    fieldLabel.setText("&Query Text:");
    fieldLabel.setVisible(false);

    queryTextField = new Text(composite, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    data = new GridData(GridData.FILL_HORIZONTAL);
    data.heightHint = 100;
    queryTextField.setLayoutData(data);
    queryTextField.addModifyListener(new ModifyListener() {
      public void modifyText(ModifyEvent e) {
        validateData();
      }
    });
    queryTextField.setVisible(false);

    validateData();

    return composite;
  }

  private void updateTableCombo() {
    tableCombo.removeAll();
    DatasourceDto datasource = getDatasource(getDatasourceName());
    for(String t : datasource.getTableList()) {
      tableCombo.add(t);
    }
    tableCombo.select(0);
  }

  private void updateQueryText() {
    StringBuffer txt = new StringBuffer();

    if(datasourceCombo.getSelectionIndex() > -1 && tableCombo.getSelectionIndex() > -1) {
      txt.append("select ");
      if(getSelectText() != null && getSelectText().trim().length() > 0) {
        txt.append("\"").append(getSelectText()).append("\"");
      } else {
        txt.append("*");
      }
      txt.append(" from '").append(getDatasourceName()).append(".").append(getTableName()).append("'");
      if(getWhereText() != null && getWhereText().trim().length() > 0) {
        txt.append(" where \"").append(getWhereText()).append("\"");
      }
    }
    queryTextField.setText(txt.toString());
  }

  private void populateDatasourceCombo() {
    try {
      URI uri = getConnection().getOpal().newUri().segment("datasources").build();
      datasources = getConnection().getOpal().getResources(DatasourceDto.class, uri, DatasourceDto.newBuilder());

      for(int i = 0; i < datasources.size(); i++) {
        DatasourceDto datasource = datasources.get(i);
        datasourceCombo.add(datasource.getName());
      }

    } catch(OdaException e) {
      e.printStackTrace();
    }
  }

  private DatasourceDto getDatasource(String name) {
    for(int i = 0; i < datasources.size(); i++) {
      DatasourceDto datasource = datasources.get(i);
      if(datasource.getName().equals(name)) {
        return datasource;
      }
    }
    return null;
  }

  private Connection getConnection() throws OdaException {
    if(connection == null || !connection.isOpen()) {
      connection = (Connection) new Driver().getConnection(null);
      Properties connProps = DesignUtil.convertDataSourceProperties(getInitializationDesign().getDataSourceDesign());
      connection.open(connProps);
    }
    return connection;
  }

  /**
   * Initializes the page control with the last edited data set design.
   */
  private void initializeControl() {
    /*
     * To optionally restore the designer state of the previous design session, use getInitializationDesignerState();
     */

    // Restores the last saved data set design
    DataSetDesign dataSetDesign = getInitializationDesign();
    if(dataSetDesign == null) return; // nothing to initialize

    String queryText = dataSetDesign.getQueryText();
    if(queryText == null) return; // nothing to initialize

    // initialize control

    if(dataSetDesign.getPublicProperties() == null) return;
    if(dataSetDesign.getPublicProperties().findProperty(Query.DATASOURCE) != null) {
      String value = dataSetDesign.getPublicProperties().findProperty(Query.DATASOURCE).getValue();
      if(value != null && value.trim().length() > 0) {
        for(int i = 0; i < datasourceCombo.getItemCount(); i++) {
          if(datasourceCombo.getItem(i).equals(value)) {
            datasourceCombo.select(i);
            break;
          }
        }
      }
    }
    if(dataSetDesign.getPublicProperties().findProperty(Query.TABLE) != null) {
      String value = dataSetDesign.getPublicProperties().findProperty(Query.TABLE).getValue();
      for(int i = 0; i < tableCombo.getItemCount(); i++) {
        if(tableCombo.getItem(i).equals(value)) {
          tableCombo.select(i);
          break;
        }
      }
    }
    if(dataSetDesign.getPublicProperties().findProperty(Query.SELECT) != null) {
      String value = dataSetDesign.getPublicProperties().findProperty(Query.SELECT).getValue();
      if(value != null) selectTextField.setText(value);
    }
    if(dataSetDesign.getPublicProperties().findProperty(Query.WHERE) != null) {
      String value = dataSetDesign.getPublicProperties().findProperty(Query.WHERE).getValue();
      if(value != null) whereTextField.setText(value);
    }

    // query text is: select <select statement: js or variable name list> from <datasource>.<table> where <where
    // statement: js>
    queryTextField.setText(queryText);

    validateData();
    setMessage(DEFAULT_MESSAGE);

    /*
     * To optionally honor the request for an editable or read-only design session, use isSessionEditable();
     */
  }

  /**
   * Get user selected datasource.
   * @return
   */
  private String getDatasourceName() {
    return datasourceCombo.getItem(datasourceCombo.getSelectionIndex());
  }

  /**
   * Get user selected table.
   * @return
   */
  private String getTableName() {
    return tableCombo.getItem(tableCombo.getSelectionIndex());
  }

  private String getSelectText() {
    return selectTextField.getText();
  }

  private String getWhereText() {
    return whereTextField.getText();
  }

  /**
   * Obtains the user-defined query text of this data set from page control.
   * @return query text
   */
  private String getQueryText() {
    return queryTextField.getText();
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#collectDataSetDesign(org.eclipse.datatools
   * .connectivity.oda.design.DataSetDesign)
   */
  protected DataSetDesign collectDataSetDesign(DataSetDesign design) {
    if(getControl() == null) // page control was never created
    return design; // no editing was done
    if(!hasValidData()) return null; // to trigger a design session error status
    savePage(design);
    return design;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#collectResponseState()
   */
  protected void collectResponseState() {
    super.collectResponseState();
    /*
     * To optionally assign a custom response state, for inclusion in the ODA design session response, use
     * setResponseSessionStatus( SessionStatus status ); setResponseDesignerState( DesignerState customState );
     */
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.eclipse.datatools.connectivity.oda.design.ui.wizards.DataSetWizardPage#canLeave()
   */
  protected boolean canLeave() {
    return isPageComplete();
  }

  /**
   * Validates the user-defined value in the page control exists and not a blank text. Set page message accordingly.
   */
  private void validateData() {
    // boolean isValid = (queryTextField != null && getQueryText() != null && getQueryText().trim().length() > 0);

    boolean isValid = datasourceCombo.getSelectionIndex() >= 0 && tableCombo.getSelectionIndex() >= 0;

    if(isValid) setMessage(DEFAULT_MESSAGE);
    else
      setMessage("Requires input value.", ERROR);

    setPageComplete(isValid);
  }

  /**
   * Indicates whether the custom page has valid data to proceed with defining a data set.
   */
  private boolean hasValidData() {
    validateData();

    return canLeave();
  }

  private java.util.Properties getBlankPageProperties() {
    java.util.Properties prop = new java.util.Properties();
    prop.setProperty(Query.DATASOURCE, "");
    prop.setProperty(Query.TABLE, "");
    prop.setProperty(Query.SELECT, "");
    prop.setProperty(Query.WHERE, "");

    return prop;
  }

  /**
   * Saves the user-defined value in this page, and updates the specified dataSetDesign with the latest design
   * definition.
   */
  private void savePage(DataSetDesign dataSetDesign) {
    // save user-defined query text
    dataSetDesign.setQueryText(getQueryText());

    if(dataSetDesign.getPublicProperties() == null || dataSetDesign.getPublicProperties().findProperty(Query.DATASOURCE) != null || dataSetDesign.getPublicProperties().findProperty(Query.TABLE) != null) {
      try {
        dataSetDesign.setPublicProperties(DesignSessionUtil.createDataSetPublicProperties(dataSetDesign.getOdaExtensionDataSourceId(), dataSetDesign.getOdaExtensionDataSetId(), getBlankPageProperties()));
      } catch(OdaException e) {
        e.printStackTrace();
      }
    }

    dataSetDesign.getPublicProperties().findProperty(Query.DATASOURCE).setNameValue(Query.DATASOURCE, getDatasourceName());
    dataSetDesign.getPublicProperties().findProperty(Query.TABLE).setNameValue(Query.TABLE, getTableName());
    dataSetDesign.getPublicProperties().findProperty(Query.SELECT).setNameValue(Query.SELECT, getSelectText());
    dataSetDesign.getPublicProperties().findProperty(Query.WHERE).setNameValue(Query.WHERE, getWhereText());

    // dataSetDesign.
    try {
      // update the data set design with the
      // query's current runtime metadata
      updateDesign(dataSetDesign);
    } catch(OdaException e) {
      // not able to get current metadata, reset previous derived metadata
      dataSetDesign.setResultSets(null);
      dataSetDesign.setParameters(null);

      e.printStackTrace();
    } finally {
      closeConnection();
    }
  }

  /**
   * Updates the given dataSetDesign with the queryText and its derived metadata obtained from the ODA runtime
   * connection.
   */
  private void updateDesign(DataSetDesign dataSetDesign) throws OdaException {
    IQuery query = getConnection().newQuery(null);
    query.prepare(getQueryText());
    query.setProperty(Query.DATASOURCE, getDatasourceName());
    query.setProperty(Query.TABLE, getTableName());
    query.setProperty(Query.SELECT, getSelectText());
    query.setProperty(Query.WHERE, getWhereText());

    try {
      IResultSetMetaData md = query.getMetaData();
      updateResultSetDesign(md, dataSetDesign);
    } catch(OdaException e) {
      // no result set definition available, reset previous derived metadata
      dataSetDesign.setResultSets(null);
      e.printStackTrace();
    }

    // proceed to get parameter design definition
    try {
      IParameterMetaData paramMd = query.getParameterMetaData();
      updateParameterDesign(paramMd, dataSetDesign);
    } catch(OdaException ex) {
      // no parameter definition available, reset previous derived metadata
      dataSetDesign.setParameters(null);
      ex.printStackTrace();
    }

    /*
     * See DesignSessionUtil for more convenience methods to define a data set design instance.
     */
  }

  /**
   * Updates the specified data set design's result set definition based on the specified runtime metadata.
   * @param md runtime result set metadata instance
   * @param dataSetDesign data set design instance to update
   * @throws OdaException
   */
  private void updateResultSetDesign(IResultSetMetaData md, DataSetDesign dataSetDesign) throws OdaException {
    ResultSetColumns columns = DesignSessionUtil.toResultSetColumnsDesign(md);

    ResultSetDefinition resultSetDefn = DesignFactory.eINSTANCE.createResultSetDefinition();
    // resultSetDefn.setName( value ); // result set name
    resultSetDefn.setResultSetColumns(columns);

    // no exception in conversion; go ahead and assign to specified dataSetDesign
    dataSetDesign.setPrimaryResultSet(resultSetDefn);
    dataSetDesign.getResultSets().setDerivedMetaData(true);
  }

  /**
   * Updates the specified data set design's parameter definition based on the specified runtime metadata.
   * @param paramMd runtime parameter metadata instance
   * @param dataSetDesign data set design instance to update
   * @throws OdaException
   */
  private void updateParameterDesign(IParameterMetaData paramMd, DataSetDesign dataSetDesign) throws OdaException {
    DataSetParameters paramDesign = DesignSessionUtil.toDataSetParametersDesign(paramMd, DesignSessionUtil.toParameterModeDesign(IParameterMetaData.parameterModeIn));

    // no exception in conversion; go ahead and assign to specified dataSetDesign
    dataSetDesign.setParameters(paramDesign);
    if(paramDesign == null) return; // no parameter definitions; done with update

    paramDesign.setDerivedMetaData(true);

    // TODO replace below with data source specific implementation;
    // hard-coded parameter's default value for demo purpose
    // if(paramDesign.getParameterDefinitions().size() > 0) {
    // ParameterDefinition paramDef = (ParameterDefinition) paramDesign.getParameterDefinitions().get(0);
    // if(paramDef != null) paramDef.setDefaultScalarValue("dummy default value");
    // }
  }

  /**
   * Attempts to close given ODA connection.
   */
  private void closeConnection() {
    try {
      if(connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch(OdaException e) {
      // ignore
      e.printStackTrace();
    } finally {
      connection = null;
    }

  }

}
