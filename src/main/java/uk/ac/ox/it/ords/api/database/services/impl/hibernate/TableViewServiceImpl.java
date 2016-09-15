/*
 * Copyright 2015 University of Oxford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ox.it.ords.api.database.services.impl.hibernate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.SimpleExpression;

import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.exceptions.ConstraintViolationException;
import uk.ac.ox.it.ords.api.database.exceptions.DBEnvironmentException;
import uk.ac.ox.it.ords.api.database.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.TableView;
import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.api.database.queries.ORDSPostgresDB;
import uk.ac.ox.it.ords.api.database.queries.ParameterList;
import uk.ac.ox.it.ords.api.database.queries.QueryRunner;
import uk.ac.ox.it.ords.api.database.services.TableViewService;

public class TableViewServiceImpl extends DatabaseServiceImpl
		implements
			TableViewService {

	@Override
	public TableData getStaticDataSetData(int dbId, int datasetId, int startIndex, int rowsPerPage, String sort, String direction)
			throws Exception {
		TableView tableView = this.getTableViewRecord(datasetId);
		OrdsPhysicalDatabase db = this.getPhysicalDatabaseFromID(dbId);
		String query = tableView.getQuery();
		String databaseName = tableView.getAssociatedDatabase();
		String server = db.getDatabaseServer();
		// run as ords superuser because this might be accessed by an unauthenticated user.
		
		QueryRunner qr = new QueryRunner(server,databaseName);
        ParameterList params = new ParameterList();
		TableData tableData = qr.runDBQuery(query, params, startIndex, rowsPerPage, true);
		return tableData;
	}

	@Override
	public int createStaticDataSetOnQuery(int dbId, TableViewInfo viewInfo) throws Exception {
		return this.createStaticRecordAndDB(dbId, 0, viewInfo);
	}

	@Override
	public int updateStaticDataSet(int dbId, int datasetId, TableViewInfo viewInfo)
			throws Exception {
		return this.createStaticRecordAndDB(dbId, datasetId, viewInfo);
	}

	@Override
	public void deleteStaticDataSet(int datasetId)
			throws Exception {
		TableView tableView = this.getTableView(datasetId);
		String databaseName = tableView.getAssociatedDatabase();

		this.removeModelObject(tableView);

		String statement = this.getTerminateStatement(databaseName);
		this.runJDBCQuery(statement, null, this.getORDSDatabaseHost(), databaseName);
		statement = "drop database " + databaseName + ";";
		this.runJDBCQuery(statement, null, this.getORDSDatabaseHost(), this.getORDSDatabaseName());
	}



	@Override
	public TableViewInfo getStaticDataSet(int datasetId) throws Exception {
		TableView tableView = this.getTableView(datasetId);
		if ( tableView == null ) {
			throw new NotFoundException("Unable to find dataset id: "+datasetId);
		}
		TableViewInfo viewInfo = new TableViewInfo(tableView);
		
		return viewInfo;
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public List<TableViewInfo> listDatasetsForDatabase(int physicalDatabaseId)
			throws Exception {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("physicalDatabaseId", physicalDatabaseId));
		List<TableView> views = (List<TableView>) this.getModelObjects(exprs, TableView.class, false);
		ArrayList<TableViewInfo> viewInfoList = new ArrayList<TableViewInfo>();
		for ( TableView v: views ) {
			TableViewInfo vi = new TableViewInfo(v);
			viewInfoList.add(vi);
		}
		return  viewInfoList;
	}
	
	

	@SuppressWarnings("unchecked")
	@Override
	public List<TableViewInfo> searchDataSets(String query) throws Exception {

		List<TableView> datasets;

		//
        // If the search has not specified any terms they will just get all open projects.
		//
		String[] terms = {};
		if ((query != null) && (query.trim().length() != 0)) {
			terms = query.split(",");
		}
		
		/*
	     * Since the user has specified search terms, we need to display all datasets that match the search terms.
		 */
		
		//
		// Get matching datasets
		//
		Session session = this.getOrdsDBSessionFactory().getCurrentSession();
		
		try {
			session.beginTransaction();
			
			Criteria searchCriteria = session.createCriteria(TableView.class)
					.add(Restrictions.eq("tvAuthorization", "public"));
			
			
			for (String term : terms) {
				searchCriteria.add(
						Restrictions.and(
								Restrictions.or(
										Restrictions.ilike("viewName", "%"+term.trim()+"%"),
										Restrictions.ilike("viewDescription", "%"+term.trim()+"%")
										)
								)
						);		
			}
			
			datasets = searchCriteria.list();
			session.getTransaction().commit();
		} catch (Exception e) {
			log.error("Error getting dataset list", e);
			session.getTransaction().rollback();
			throw e;
		} finally {
			  HibernateUtils.closeSession();
		}		

		List<TableViewInfo> response = new ArrayList<TableViewInfo>();
		
		for (TableView dataset : datasets){
			TableViewInfo info = new TableViewInfo(dataset);
			response.add(info);
		}
		
		return response;
		
	}
	
	

	@Override
	public TableView getTableViewRecord(int tableViewId) {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("id", tableViewId));
		return this.getModelObject(exprs, TableView.class, false);
	}
	

	@Override
	public TableData getDatabaseRows(int dbId,
			String tableName, int startIndex, int rowsPerPage,
			String sort, String sortDirection) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		
		ORDSPostgresDB sqlQueries = new ORDSPostgresDB(database.getDatabaseServer(), database.getDbConsumedName() );
		boolean direction = false;
		if (sortDirection != null && sortDirection.equalsIgnoreCase("ASC") ){
			direction = true;
		}
		TableData tableData = sqlQueries.getTableDataForTable(tableName, startIndex, rowsPerPage, sort, direction);
		
		// get the actual number of rows in table
		
		return tableData;
	}

	@Override
	public boolean appendTableData(int dbId, String tableName,
			Row newData) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		
		return addRowToTable(database, tableName, newData.columnNames, newData.values);
	}
	
	/**
	 * Add a row of data to a table
	 *
	 * @param tableName
	 *            the table where the row data should be added
	 * @param cols
	 *            an array of columns where the data is to reside
	 * @param cellData
	 *            an array of the data values. If a cell contains
	 *            Vars.NULL_VALUE then it is assumed to be a null value.
	 * @return true if successful
	 * @throws Exception 
	 */
	private boolean addRowToTable(OrdsPhysicalDatabase database, String tableName, String[] cols,
			String[] cellData) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug(String.format("addRowToTable(%s)", tableName));
		}
		
		ORDSPostgresDB sqlQueries = new ORDSPostgresDB( database.getDatabaseServer(), database.getDbConsumedName() );

		String primaryKey = sqlQueries.getSingularPrimaryKeyColumn(tableName);

		if (cols == null) {
			log.error("No cols to add");
			return false;
		} else if (cellData == null) {
			log.error("No cellData to add");
			return false;
		}

		if (cellData.length == 0) {
			cellData = new String[cols.length];
		}
		
		if (cellData.length != cols.length){
			log.error("Input contained different lengths for data and columns");
			return false;
		}

		/*
		 * If the user specifies '' then a blank is added. If the user specifies
		 * Vars.NULL_VALUE then a null is added. If the user specifies nothing
		 * (i.e.
		 * 
		 * Then the default value is used
		 */
		ArrayList<String> colList = new ArrayList<String>(Arrays.asList(cols));
		ArrayList<String> dataList = new ArrayList<String>(Arrays.asList(cellData));
		ArrayList<String> empties = new ArrayList<String>();
		String columns = "";
		String values = "";
		for (int index = 0; index < cols.length; index++) {
			if ((cellData[index] != null)
					&& (cellData[index].equals("[null value]"))) {
				log.warn("Null value in cell data");
				/*
				 * Null value
				 */
			}
			if (cellData[index] == null) {
				/*
				 * When data is passed in to ORDS, if nothing has been specified
				 * by the user then the values should be blank. However I
				 * sometimes see them as NULL. I don't currently know why.
				 */
				//TODO What is going on here???
				log.warn("Unexpected value of null here. This should not happen - let's make the value a blank for now.");
				cellData[index] = null;
//				continue;
			} else if (cellData[index].isEmpty()) {
				// Ignore this - let the database use the default value
				colList.remove(cols[index]);
				empties.add(cellData[index]);
				continue;
			}
			columns += "\"" + cols[index].trim() + "\",";
			values += "?,";
		}
		dataList.removeAll(empties);

		if (columns.isEmpty()) {
			/*
			 * This is a special case. Normally, if the user omits data in a
			 * column, the default value for that column will be used. However,
			 * That cannot happen for cases where the user omits all data in all
			 * columns, because then out insert command becomes invalid. So we
			 * assume this is a no-op and return true.
			 */
			log.info("No data entered - blanks needed");
			String command = String.format("insert into \"%s\" default values",
					tableName);
			return sqlQueries.runDBQuery(command);
		}

		if (columns.endsWith(",")) {
			columns = columns.substring(0, columns.length() - 1);
		}
		if (values.endsWith(",")) {
			values = values.substring(0, values.length() - 1);
		}

		//
		// Note that "values" here is actually "? ? ?" placeholders to be replaced in a prepared statement
		//
		String command = String.format("insert into \"%s\" (%s) values (%s)",
				tableName, columns, values);

		//
		// Get the map of datatypes for this row and populate it with values for the row
		//		
		ParameterList parameterList = this.createParameterList(database, tableName, colList, dataList);

		// Insert blank - insert into table1 (col1,col2,col3,col4) values
		// ('a','b','c','');
		// Insert null - insert into table1 (col1,col2,col3) values
		// ('a','b','c');
		// or insert into table1 (col1,col2,col3,col4) values
		// ('a','b','c',null);

		boolean ret = false;
		
		QueryRunner dq = new QueryRunner(database.getDatabaseServer(), database.getDbConsumedName());
		
		ret = dq.createAndExecuteStatement(command, parameterList, tableName);

		if (!ret) {
			log.warn("Unable to update table successfully due to error.");
			log.warn("Trying again after resetting the sequence");
			if (!resetSequence(database.getDatabaseServer(), database.getDbConsumedName(), tableName, primaryKey)) {
				log.error("Unable to reset the sequence - will try the insert again, just in case");
			}
			ret = dq.createAndExecuteStatement(command, parameterList, tableName);
		}

		return ret;

	}
	
	private boolean resetSequence(String server, String databaseName, String tableName, String index)
			throws ClassNotFoundException, DBEnvironmentException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("resetSequence(%s, %s)", tableName, index));
		}

		// Example:
		// SELECT setval(pg_get_serial_sequence('tblinvdetail','invdetailinvnum'),(SELECT GREATEST(MAX(invdetailinvnum)+1,nextval(pg_get_serial_sequence('tblinvdetail','invdetailinvnum')))-1 FROM tblinvdetail))
//		String sequenceResetCommand = String
//				.format("SELECT pg_catalog.setval(pg_get_serial_sequence('%s', '%s'), (SELECT MAX(%s) FROM \"%s\"));",
//						tableName, index, index, tableName);
		String sequenceResetCommand = String
				.format("SELECT setval(pg_get_serial_sequence('%s','%s'),(SELECT GREATEST(MAX(%s)+1,nextval(pg_get_serial_sequence('%s','%s')))-1 FROM %s));",
						tableName, index, index, tableName, index, tableName);
		boolean ret = false;
		try {
			ret = new ORDSPostgresDB(server, databaseName).runDBQuery(sequenceResetCommand);
		} catch (SQLException ex) {
			log.error("Exception", ex);
		}

		return ret;
	}


	@Override
	public int updateTableRow(int dbId, String tableName, List<Row> rowDataList)
			throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		
		if ( database == null ) {
			throw new NotFoundException();
		}

		for ( Row rowData: rowDataList ) {
			String command = "UPDATE \""+ tableName + "\" SET ";
			
			for ( String colName: rowData.columnNames) {
				command += "\""+colName + "\"= ?,";
			}
			
			// take off last comma
			command = command.substring(0, command.length()-1);
			
			command += " WHERE \""+rowData.lookupColumn+"\"=";
			if (isInt(rowData.lookupValue ) || isNumber(rowData.lookupValue)) {
				command += rowData.lookupValue;
			}
			else {
				command += "'"+rowData.lookupValue+"'";
			}
			
			//
			// Get the map of datatypes for this row and populate it with values for the row
			//		
			List<String> colList = Arrays.asList(rowData.columnNames);
			List<String> dataList = Arrays.asList(rowData.values);			
			ParameterList parameterList;
			
			try {
				
				parameterList = this.createParameterList(database, tableName, colList, dataList);
				
			} catch (Exception e) {
				
				log.error(e.getMessage());
				
				throw new NotFoundException();
				
			}
			
			QueryRunner dq = new QueryRunner(database.getDatabaseServer(), database.getDbConsumedName());
			try {
				dq.createAndExecuteStatement(command, parameterList, tableName);
			} catch (Exception e) {
				
				log.error(e.getMessage());
				
				throw new BadParameterException(command);
			}
		}

		return 0;
	}

	@Override
	public void deleteTableData(int dbId, String tableName,
			String primaryKey, String primaryKeyValue) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		if (primaryKey == null || primaryKey.isEmpty() || primaryKeyValue == null || primaryKeyValue.isEmpty()){
			throw new BadParameterException("Missing primary key data in delete row request");
		}
		Row rowToRemove = new Row();
		rowToRemove.columnNames = new String[]{primaryKey};
		rowToRemove.values = new String[]{primaryKeyValue};
		deleteTableRow(database, tableName, rowToRemove);
	}
	
	/**
	 * Delete a row from a table
	 * @param database 
	 * 			  the database to delete the row from
	 * @param tableName
	 *            the name of the table to operate on
	 * @param row
	 *            the row to delete
	 * @return true if successful, else false
	 * @throws Exception 
	 *            if there is a problem deleting the row
	 */
	private boolean deleteTableRow(OrdsPhysicalDatabase database, String tableName, Row row) throws Exception {
		if (tableName == null) {
			log.error("Null table name - can't be having that");
			return false;
		}
				
		if (log.isDebugEnabled()) {
			log.debug("deleteTableRow for table " + tableName);
			log.debug("These are the data columns to be deleted");
			log.debug("This is the data to be deleted");
			int i = 0;
			for (String name : row.columnNames) {
				log.debug("Deleting column: " + name + "data: "+row.values[i++]);
			}
		}

		String command = String.format("delete from \"%s\" where ", tableName);
		String whereString = "";
		int index = 0;

		for (String columnName : row.columnNames) {

			//log.debug("Calling replace from delete row routine");
			//whereString += String.format("\"%s\" = '%s' ", columnName,
			//			this.replaceSpecialCharacters(row.values[index++]));
			index++;
			whereString += String.format("\"%s\" = ? ",columnName);
			
			if (index < row.columnNames.length) {
				whereString += " and ";
			}
			else {
				whereString += ";";
			}
		}
		command += whereString;

		if (log.isDebugEnabled()) {
			log.debug("Command to run:" + command);
		}

		boolean ret = false;
		
		//
		// Convert column names and values into Lists so we can iterate through them more easily
		//
		ArrayList<String> colList = new ArrayList<String>(Arrays.asList(row.columnNames));
		ArrayList<String> dataList = new ArrayList<String>(Arrays.asList(row.values));

		//
		// Get the map of datatypes for this row and populate it with values for the row
		//		
		ParameterList parameterList = this.createParameterList(database, tableName, colList, dataList);
		
		//
		// Execute the delete statement
		//
		try {
			new QueryRunner(database.getDatabaseServer(), database.getDbConsumedName()).createAndExecuteStatement(command, parameterList, tableName);
		} catch (Exception e) {
			
			//
			// If we get an SQL exception, if its one we recognise, wrap it in a custom exception to throw back up the chain
			//
			if (e instanceof SQLException){
				if (e.getMessage().contains("violates foreign key constraint")){
					throw new ConstraintViolationException(e.getMessage());
				}
			} else {
				//
				// Otherwise just throw the exception
				//
				throw e;
			}
		}

		return ret;
	}
	
	/**
	 * Populates a parameter lisst with processed values. Used when constructing parameter lists
	 * for a prepared statement from arrays of values for a row.
	 * @param dataTypeMaps
	 * @param columns
	 * @param data
	 * @return
	 */
	private ParameterList createParameterList(OrdsPhysicalDatabase database, String table, List<String> columns, List<String> data) throws Exception{

		ParameterList parameterList = new ParameterList();

		Map<String, DataTypeMap> dataTypeMaps = getDataTypeMaps(database, table);

		if (dataTypeMaps == null || dataTypeMaps.isEmpty()) throw new NotFoundException();

		for (DataTypeMap dataTypeMap : dataTypeMaps.values()) {
			
			//
			// get the index of the column
			//
			int colIndex = columns.indexOf(dataTypeMap.colName);
			
			if (colIndex > -1) {
				
				//
				// Set the index of the parameter. Note that
				// indexes of parameters start at 1 not 0.
				//
				dataTypeMap.index = colIndex + 1;
				if ("[null value]".equals(
						data.get(colIndex))) {
					dataTypeMap.stringValue = null;
				} else {
					dataTypeMap.stringValue = data.get(colIndex);
				}
				parameterList.addParameter(dataTypeMap);
			}
		}
		
		return parameterList;
	}
	
	/**
	 * Returns the data type map (a map of types for values) for a table
	 * @param tableName
	 * @return
	 * @throws Exception if the type map cannot be obtained - this is usually because the table specified does not exist
	 */
	private Map<String, DataTypeMap> getDataTypeMaps(OrdsPhysicalDatabase database, String tableName)
			throws Exception {
		if (log.isDebugEnabled()) {
			log.debug(String.format("getDataTypeMaps(%s)", tableName));
		}

		Map<String, DataTypeMap> dataTypeMappingList = new ConcurrentHashMap<String, DataTypeMap>();
		QueryRunner qr = new QueryRunner(database.getDatabaseServer(), database.getDbConsumedName());
		qr.runDBQuery("SELECT column_name, data_type FROM information_schema.columns where table_name=\'"
				+ tableName + "\'");

		for (DataRow rows : qr.getTableData().rows) {
			DataTypeMap dtm = new DataTypeMap();
			dtm.colName = rows.cell.get("column_name").getValue();
			dtm.dt = QueryRunner.getDataType(rows.cell.get("data_type")
					.getValue());
			dataTypeMappingList.put(dtm.colName, dtm);
		}		

		return dataTypeMappingList;
	}

	protected int createStaticRecordAndDB(int dbId, int datasetId,
			TableViewInfo viewInfo) throws Exception {
		if ( !this.isQueryAllowed(viewInfo.getViewQuery())) {
			throw new BadParameterException("Only select queries allowed on datasets");
		}
		OrdsPhysicalDatabase physicalDatabase = this.getPhysicalDatabaseFromID(dbId);
		OrdsDB logicalDatabase = this.getLogicalDatabaseFromID(physicalDatabase.getLogicalDatabaseId());
		Subject s = SecurityUtils.getSubject();
		String principalName = s.getPrincipal().toString();
		User u = this.getUserByPrincipal(principalName);
		
		if ( datasetId != 0 ) {
			// remove old static copy
			this.dropDatasetDatabase(datasetId);
		}
		String staticDBName = this.generateStaticDBName(physicalDatabase.getDbConsumedName());

		TableView viewRecord = this.getTableView(datasetId);
		
		viewRecord.setViewName(viewInfo.getViewName());
		viewRecord.setProjectId(logicalDatabase.getDatabaseProjectId());
		viewRecord.setViewDescription(viewInfo.getViewDescription());
		viewRecord.setAssociatedDatabase(staticDBName);
		viewRecord.setCreatorId(u.getUserId());
		viewRecord.setQuery(viewInfo.getViewQuery());
		viewRecord.setPhysicalDatabaseId(physicalDatabase.getPhysicalDatabaseId());
		viewRecord.setStaticDataset(true);
		viewRecord.setOriginalDatabase(physicalDatabase.getDbConsumedName());
		viewRecord.setAssociatedTable(viewInfo.getViewTable());

		viewRecord.setTvAuthorization(viewInfo.getViewAuthorization());
		// create a copy of the database
		this.copyStatic(physicalDatabase.getDbConsumedName(), staticDBName, physicalDatabase.getDatabaseServer());
		viewRecord.setOriginalDatabase(physicalDatabase.getDbConsumedName());
		if ( datasetId == 0 ) {
			this.saveModelObject(viewRecord);
		}
		else {
			this.updateModelObject(viewRecord);
		}
		return viewRecord.getId();
	}

	
	
	
	
    private boolean isQueryAllowed(String query) {
        log.debug("isQueryAllowed");
        
        boolean ret = false;
        
        if (query != null) {
            String strippedQuery = query.toLowerCase().trim();
            if (strippedQuery.startsWith("select")) {
                // ok so far
                if ( (query.contains("update")) || (query.contains("grant")) ||
                    (query.contains("delete")) ) {
                    // bad
                }
                else {
                    ret = true;
                }
            }
        }

        
        return ret;
    }

	
	
	
	
    private TableView getTableView(int viewId) throws Exception {
        TableView tv;
        if (viewId == 0) {
            tv = new TableView();
        }
        else {
            tv = this.getTableViewRecord(viewId);
        }

        return tv;
    }
    
  
    
    
    
    private  String generateStaticDBName(String dbName) throws Exception{
        log.debug(String.format("generateStaticDBName(%s)", dbName));

        String possibleDbName;
        int counter = 1;

        while (true) {
            possibleDbName = dbName + "_" + counter + "_static";
            if (!this.checkDatabaseExists(possibleDbName)) {
                break;
            }
            counter++;
        }

        return possibleDbName;
    }
    
    
	private void copyStatic (String from, String to, String server ) throws Exception {
//		String userName = this.getODBCUser();
//		String password = this.getODBCPassword();
//		this.createOBDCUserRole(userName, password);
		
		if (this.checkDatabaseExists(to)) {
			String statement = this.getTerminateStatement(to);
			this.runJDBCQuery(statement, null, server, null);
			statement = "rollback transaction; drop database " + to + ";";
			this.runJDBCQuery(statement, null, server, null);
		}
		String clonedb = String.format(
				"ROLLBACK TRANSACTION; CREATE DATABASE %s WITH TEMPLATE %s",
				quote_ident(to),
				quote_ident(from));
		this.runJDBCQuery(clonedb, null, server, null);
	
	}
	
	
	private void dropDatasetDatabase ( int datasetId) throws Exception {
		TableView tableView = this.getTableView(datasetId);
		String databaseName = tableView.getAssociatedDatabase();


		String statement = this.getTerminateStatement(databaseName);
		this.runJDBCQuery(statement, null, this.getORDSDatabaseHost(), databaseName);
		statement = "drop database " + databaseName + ";";
		this.runJDBCQuery(statement, null, this.getORDSDatabaseHost(), this.getORDSDatabaseName());

	}



	
	
	private boolean isInt ( String input ) {
		try {
			Integer.parseInt(input);
		}
		catch ( NumberFormatException e ) {
			return false;
		}
		return true;
	}
	
	
	private boolean isNumber ( String input ) {
		try {
			Float.parseFloat(input );
		}
		catch ( NumberFormatException e ) {
			return false;
		}
		return true;
	}


}
