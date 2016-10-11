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

import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.queries.ORDSPostgresDB;
import uk.ac.ox.it.ords.api.database.queries.ParameterList;
import uk.ac.ox.it.ords.api.database.queries.QueryRunner;
import uk.ac.ox.it.ords.api.database.services.QueryService;

public class QueryServiceImpl extends DatabaseServiceImpl
		implements
			QueryService {


	@Override
	public TableData performQuery(int dbId, String q,
			int startIndex, int rowsPerPage, String filter, String order)
			throws Exception {
		
		if (q == null || q.trim().isEmpty()){
			throw new BadParameterException("No query supplied");
		}
		
		OrdsPhysicalDatabase db = this.getPhysicalDatabaseFromID(dbId);
		String dbName = db.getDbConsumedName();
		String server = db.getDatabaseServer();
		
		QueryRunner qr = new QueryRunner(server,dbName);
		TableData data;
		try {
			ParameterList params = new ParameterList();
			data = qr.runDBQuery(q, params, startIndex, rowsPerPage, true);
		} catch (Exception e) {
			throw new BadParameterException("Invalid query");
		}
		return data;
	}

	@Override
	public TableData getReferenceColumnData(int dbId,
			String table, String foreignKeyColumn, String term) throws Exception {
		OrdsPhysicalDatabase db = this.getPhysicalDatabaseFromID(dbId);
		String dbName = db.getDbConsumedName();
		String server = db.getDatabaseServer();
		
		ORDSPostgresDB qr = new ORDSPostgresDB(server, dbName);
		
		if (term != null && !term.isEmpty())
			return qr.getReferenceValues(table, foreignKeyColumn, term);
		else
			return qr.getReferenceValues(table, foreignKeyColumn);
	}

}
