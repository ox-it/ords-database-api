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
import java.util.List;

import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase.EntityType;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase.ImportType;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissionSets;
import uk.ac.ox.it.ords.api.database.services.AccessImportService;
import uk.ac.ox.it.ords.api.database.services.CSVService;
import uk.ac.ox.it.ords.api.database.services.DatabaseRoleService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.services.PermissionsService;

public class DatabaseUploadServiceImpl extends DatabaseServiceImpl
		implements
			DatabaseUploadService {

	@Override
	public int createNewDatabaseFromFile(int logicalDatabaseId, File dbFile, String type,
			String server) throws Exception {
		String odbcUserName = this.getODBCUser();
		String odbcPassword = this.getODBCPassword();
		OrdsPhysicalDatabase db = this.createPhysicalDatabase(logicalDatabaseId, type, dbFile.getName(), dbFile.length(), server, odbcUserName, odbcPassword);
		String databaseName = db.getDbConsumedName();
		DatabaseRoleService.Factory.getInstance().createInitialPermissions(db);
		
		if ( type == null ) {
			throw new BadParameterException("No type for uploaded file");
		}
		
		if ( type.equalsIgnoreCase("csv")) {
			CSVService service = CSVService.Factory.getInstance();
			service.newTableDataFromFile(server, databaseName, dbFile, true, odbcUserName, odbcPassword);
		}
		else {
			AccessImportService service = AccessImportService.Factory.getInstance();
			service.preflightImport(dbFile);
			service.createSchema(server, databaseName, dbFile, odbcUserName);
			service.importData(server, databaseName, dbFile, odbcUserName);
		}
		return db.getPhysicalDatabaseId();
	}

	@Override
	public void init() throws Exception {
		initializePermissions();
		
	}
	
	
	private OrdsPhysicalDatabase createPhysicalDatabase ( int logicalDatabaseId, String databaseFileType, String fileName, long fileSize, String server, String userName, String password ) throws Exception {
		OrdsPhysicalDatabase db = new OrdsPhysicalDatabase();
		db.setLogicalDatabaseId(logicalDatabaseId);
		db.setEntityType(EntityType.MAIN);
		db.setImportProgress(ImportType.IN_PROGRESS);
		db.setDatabaseServer(server);
		db.setFileName(fileName);
		db.setFileSize(fileSize);
		db.setFullPathToDirectory(System.getProperty("java.io.tmpdir")
				+ "/databases");
		db.setDatabaseType(databaseFileType);
		
		this.saveModelObject(db);
		if ( db.getPhysicalDatabaseId() == 0) {
			throw new Exception("Cannot retrieve database ID in newly created record");
		}
		 else {
			this.createOBDCUserRole(userName, password);
			String dbName = db.getDbConsumedName();
			String statement = String.format(
					"rollback transaction;create database %s owner = \"%s\";",
					dbName, userName);

			this.runSQLStatementOnOrdsDB(statement);
			String createSequence = "CREATE SEQUENCE ords_constraint_seq";
			this.runJDBCQuery(createSequence, null, server, dbName);

			return db;
		}
	}
	
	
	
	
	
	
	   private void initializePermissions() throws Exception {
			PermissionsService service = PermissionsService.Factory.getInstance();
			//
			// Anyone with the "User" role can contribute to projects
			//
			for (String permission : DatabasePermissionSets.getPermissionsForUser()){
				Permission permissionObject = new Permission();
				permissionObject.setRole("user");
				permissionObject.setPermission(permission);
				service.createPermission(permissionObject);
			}
			
			//
			// Anyone with the "LocalUser" role can create new trial projects
			//
			for (String permission : DatabasePermissionSets.getPermissionsForLocalUser()){
				Permission permissionObject = new Permission();
				permissionObject.setRole("localuser");
				permissionObject.setPermission(permission);
				service.createPermission(permissionObject);
			}
			
			//
			// Anyone with the "Administrator" role can create new full
			// projects and upgrade projects to full, and update any
			// user projects
			//
			for (String permission : DatabasePermissionSets.getPermissionsForSysadmin()){
				Permission permissionObject = new Permission();
				permissionObject.setRole("administrator");
				permissionObject.setPermission(permission);
				service.createPermission(permissionObject);
			}

			//
			// "Anonymous" can View public projects
			//
			for (String permission : DatabasePermissionSets.getPermissionsForAnonymous()){
				Permission permissionObject = new Permission();
				permissionObject.setRole("anonymous");
				permissionObject.setPermission(permission);
				service.createPermission(permissionObject);
			}

	    }

	@Override
	public void testDeleteDatabase(int dbId, String instance, boolean staging) throws Exception {
		// TODO Auto-generated method stub
		OrdsPhysicalDatabase database = getPhysicalDatabaseFromIDInstance(dbId,
				instance);
		String databaseName;
		if (!staging) {
			databaseName = database.getDbConsumedName();
			this.removeModelObject(database);
		} else {
			databaseName = this.calculateStagingName(database
					.getDbConsumedName());
		}
		String statement = this.getTerminateStatement(databaseName);
		this.singleResultQuery(statement);
		statement = "rollback transaction; drop database " + databaseName + ";";
		this.runSQLStatementOnOrdsDB(statement);
		
	}
	


}
