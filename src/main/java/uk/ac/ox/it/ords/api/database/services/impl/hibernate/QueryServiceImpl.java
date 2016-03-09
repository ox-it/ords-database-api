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

import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.queries.QueryRunner;
import uk.ac.ox.it.ords.api.database.services.QueryService;

public class QueryServiceImpl extends DatabaseServiceImpl
		implements
			QueryService {


	@Override
	public TableData performQuery(int dbId, String instance, String q,
			int startIndex, int rowsPerPage, String filter, String order)
			throws Exception {
		OrdsPhysicalDatabase db = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String dbName = db.getDbConsumedName();
		String server = db.getDatabaseServer();
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
		
		QueryRunner qr = new QueryRunner(server,dbName,userName, password);
		qr.runDBQuery(q, startIndex, rowsPerPage);
		return qr.getTableData();
	}

	@Override
	public TableData getReferenceColumnData(int dbId, String instance,
			String table, String foreignKeyColumn) throws Exception {
		OrdsPhysicalDatabase db = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		String dbName = db.getDbConsumedName();
		String server = db.getDatabaseServer();
		String userName = this.getODBCUser();
		String password = this.getODBCPassword();
        QueryRunner qr = new QueryRunner(server,dbName, userName, password);

		TableData referenceValues = new TableData();
        try {
            String primaryKey = getSingularPrimaryKeyColumn(table, qr);
            String query = String.format("SELECT DISTINCT \"%s\" AS value, \"%s\" AS label FROM \"%s\" ORDER BY label ASC", primaryKey, foreignKeyColumn, table);
            qr.runDBQuery(query);
            referenceValues = qr.getTableData();
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage());
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return referenceValues;
	}
	
	
	   protected String getSingularPrimaryKeyColumn(String tableName, QueryRunner qr) throws ClassNotFoundException, SQLException {
	       log.debug("getSingularPrimaryKeyColumn");
	       String command = String
	               .format("SELECT pg_attribute.attname,  format_type(pg_attribute.atttypid, pg_attribute.atttypmod)  FROM pg_index, pg_class, pg_attribute WHERE pg_class.oid = '%s'::regclass AND indrelid = pg_class.oid AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey) AND indisprimary",
	               tableName);

	       if (!qr.runDBQuery(command)) {
	    	   return null;
	       }

	       /*
	        * Note We shall currently restrict this function to only return the
	        * primary key if there is a single primary key. Maybe we can extend that
	        * later
	        */
	       if (qr.getTableData().rows.size() >= 1) {
	           for (DataRow dr : qr.getTableData().rows) {
	               String primaryKey = dr.cell.get("attname").getValue();
	               return primaryKey;
	           }
	       }

	       return null;
	   }


}