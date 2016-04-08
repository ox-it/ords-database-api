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

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.NotFoundException;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.SimpleExpression;

import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.TableView;
import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.api.database.queries.DatabaseQueries;
import uk.ac.ox.it.ords.api.database.queries.GeneralSQLQueries;
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
		String userName = this.getORDSDatabaseUser();
		String password = this.getORDSDatabasePassword();
		
		QueryRunner qr = new QueryRunner(server,databaseName,userName, password);
		qr.runDBQuery(query, startIndex, rowsPerPage);
		TableData tableData =  qr.getTableData();
		
		
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
	public void deleteStaticDataSet(int dbId, int datasetId)
			throws Exception {
		TableView tableView = this.getTableView(datasetId);
		String databaseName = tableView.getAssociatedDatabase();
		String statement = this.getTerminateStatement(databaseName);
		this.singleResultQuery(statement);
		
		statement = "rollback transaction; drop database " + databaseName + ";";
		this.runSQLStatementOnOrdsDB(statement);
		
		this.removeModelObject(tableView);
	}



	@Override
	public TableViewInfo getStaticDataSet(int datasetId) throws Exception {
		TableViewInfo viewInfo = new TableViewInfo();
		TableView tableView = this.getTableView(datasetId);
		if ( tableView == null ) {
			throw new NotFoundException("Unable to find dataset id: "+datasetId);
		}
		viewInfo.setViewAuthorization(tableView.getTvAuthorization());
		viewInfo.setViewDescription(tableView.getViewDescription());
		viewInfo.setViewName(tableView.getViewName());
		viewInfo.setViewQuery(tableView.getQuery());
		viewInfo.setViewTable(tableView.getAssociatedTable());
		
		return viewInfo;
	}
	
	

	@Override
	public TableView getTableViewRecord(int tableViewId) {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("id", tableViewId));
		return this.getModelObject(exprs, TableView.class);
	}
	

	@Override
	public TableData getDatabaseRows(int dbId,
			String tableName, int startIndex, int rowsPerPage,
			String sort, String sortDirection) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		
		GeneralSQLQueries sqlQueries = new GeneralSQLQueries(null,database.getDbConsumedName(), userName, password );
		boolean direction = false;
		if (sortDirection != null && sortDirection.equalsIgnoreCase("ASC") ){
			direction = true;
		}
		TableData tableData = sqlQueries.getTableDataForTable(tableName, startIndex, rowsPerPage, sort, direction);
		
		// get the actual number of rows in table
		
		return tableData;
	}

	@Override
	public int appendTableData(int dbId, String tableName,
			Row newData) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}

		String server = database.getDatabaseServer();

		GeneralSQLQueries sqlQueries = new GeneralSQLQueries(server, database.getDbConsumedName(), userName, password );
		// create a database queries that points by default to the local ordsdb
		DatabaseQueries dq = new DatabaseQueries(server, database.getDbConsumedName(), userName, password);
		sqlQueries.addRowToTable(tableName, newData.columnNames, newData.values, dq);
		
		return 0;
	}

	@Override
	public int updateTableRow(int dbId, String tableName, List<Row> rowDataList)
			throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		String server = database.getDatabaseServer();
		for ( Row rowData: rowDataList ) {
			int i = 0;
			String command = "UPDATE \""+ tableName + "\" SET ";
			for ( String colName: rowData.columnNames) {
				command += "\""+colName + "\"=";
				String colVal = rowData.values[i++];
				if ( isInt(colVal) || isNumber(colVal) ) {
					command += colVal+",";
				}
				else {
					command += "\'"+ colVal + "\',";
				}
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
			
			QueryRunner q = new QueryRunner ( server, database.getDbConsumedName(), userName, password );
			
			q.runDBQuery(command);
		}

		return 0;
	}

	@Override
	public void deleteTableData(int dbId, String tableName,
			String primaryKey, String primaryKeyValue) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		if ( database == null ) {
			throw new NotFoundException();
		}
		GeneralSQLQueries sqlQueries = new GeneralSQLQueries(null, database.getDbConsumedName(), userName, password );
		Row rowToRemove = new Row();
		rowToRemove.columnNames = new String[]{primaryKey};
		rowToRemove.values = new String[]{primaryKeyValue};
		sqlQueries.deleteTableRow(tableName, rowToRemove);
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
		this.copyStatic(physicalDatabase.getDbConsumedName(), staticDBName);
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
    
    
	private void copyStatic (String from, String to ) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		this.createOBDCUserRole(userName, password);
		
		if (this.checkDatabaseExists(to)) {
			String statement = this.getTerminateStatement(to);
			this.runSQLStatementOnOrdsDB(statement);
			statement = "rollback transaction; drop database " + to + ";";
			this.runSQLStatementOnOrdsDB(statement);
		}
		String clonedb = String.format(
				"ROLLBACK TRANSACTION; CREATE DATABASE %s WITH TEMPLATE %s OWNER = %s",
				quote_ident(to),
				quote_ident(from),
				quote_ident(userName));
		this.runSQLStatementOnOrdsDB(clonedb);
	
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
