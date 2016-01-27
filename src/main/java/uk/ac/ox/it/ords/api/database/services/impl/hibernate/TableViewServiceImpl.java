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

import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.queries.DatabaseQueries;
import uk.ac.ox.it.ords.api.database.queries.GeneralSQLQueries;
import uk.ac.ox.it.ords.api.database.queries.QueryRunner;
import uk.ac.ox.it.ords.api.database.services.TableViewService;

public class TableViewServiceImpl extends DatabaseServiceImpl
		implements
			TableViewService {

	@Override
	public TableData getStaticDataSet(int dbId, String instance, int datasetId)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int createStaticDataSetOnQuery(int dbId, String instance,
			String query) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int updateStaticDataSet(int dbId, String instance, String query)
			throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void deleteStaticDataSet(int dbId, String instance, int datasetId)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public TableData getDatabaseRows(int dbId, String instance,
			String tableName, int startIndex, int rowsPerPage,
			String sort, String sortDirection) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		
		GeneralSQLQueries sqlQueries = new GeneralSQLQueries(null,database.getDbConsumedName(), userName, password );
		boolean direction = false;
		if (sortDirection != null && sortDirection.equalsIgnoreCase("ASC") ){
			direction = true;
		}
		TableData tableData = sqlQueries.getTableDataForTable(tableName, startIndex, rowsPerPage, sort, direction);
		
		return tableData;
	}

	@Override
	public int appendTableData(int dbId, String instance, String tableName,
			Row newData) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		
		GeneralSQLQueries sqlQueries = new GeneralSQLQueries(null, database.getDbConsumedName(), userName, password );
		// create a database queries that points by default to the local ordsdb
		DatabaseQueries dq = new DatabaseQueries(null);
		sqlQueries.addRowToTable(tableName, newData.columnNames, newData.values, dq);
		
		return 0;
	}

	@Override
	public int updateTableRow(int dbId, String instance, String tableName, Row rowData)
			throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String server = database.getDatabaseServer();
		int i = 0;
		String command = "UPDATE \""+ tableName + "\" SET ";
		for ( String colName: rowData.columnNames) {
			command += colName + "=";
			String colVal = rowData.values[i++];
			if ( isInt(colVal) || isNumber(colVal) ) {
				command += colVal;
			}
			else {
				command += "\'"+ colVal + "\',";
			}
		}
		// take off last comma
		command = command.substring(0, command.length()-1);
		
		command += " WHERE "+rowData.lookupColumn+"=";
		if (isInt(rowData.lookupValue ) || isNumber(rowData.lookupValue)) {
			command += rowData.lookupValue;
		}
		else {
			command += "'"+rowData.lookupValue+"'";
		}
		
		QueryRunner q = new QueryRunner ( server, database.getDbConsumedName(), userName, password );
		
		q.runDBQuery(command);

		return 0;
	}

	@Override
	public void deleteTableData(int dbId, String instance, String tableName,
			Row rowToRemove) throws Exception {
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		
		GeneralSQLQueries sqlQueries = new GeneralSQLQueries(null, database.getDbConsumedName(), userName, password );
		sqlQueries.deleteTableRow(tableName, rowToRemove);
	}
	
	
	private boolean isInt ( String input ) {
		try {
			int i = Integer.parseInt(input);
		}
		catch ( NumberFormatException e ) {
			return false;
		}
		return true;
	}
	
	
	private boolean isNumber ( String input ) {
		try {
			float f = Float.parseFloat(input );
		}
		catch ( NumberFormatException e ) {
			return false;
		}
		return true;
	}

}
