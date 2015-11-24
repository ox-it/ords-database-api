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

import java.io.File;

import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.AccessImportService;
import uk.ac.ox.it.ords.api.database.services.CSVService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;

public class DatabaseUploadServiceImpl extends DatabaseServiceImpl
		implements
			DatabaseUploadService {

	@Override
	public int createNewDatabaseFromFile(int physicalDBId, File dbFile,
			String type) throws Exception {
		String odbcUserName = this.getODBCUser();
		String odbcPassword = this.getODBCPassword();
		OrdsPhysicalDatabase db = this.getPhysicalDatabaseFromIDInstance(physicalDBId, "MAIN");
		String databaseName = db.getDbConsumedName();
		
		if ( type == null ) {
			throw new BadParameterException("No type for uploaded file");
		}
		
		if ( type.equalsIgnoreCase("csv")) {
			CSVService service = CSVService.Factory.getInstance();
			service.newTableDataFromFile(null, databaseName, dbFile, true, odbcUserName, odbcPassword);
		
		}
		else {
			AccessImportService service = AccessImportService.Factory.getInstance();
			service.preflightImport(dbFile);
			service.createSchema(null, databaseName, dbFile, odbcUserName);
		}
		return 0;
	}

}
