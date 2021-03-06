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
import java.sql.SQLException;

import org.apache.shiro.SecurityUtils;
import org.hibernate.Session;
import org.hibernate.Transaction;

import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.exceptions.UploadSizeException;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase.EntityType;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase.ImportType;
import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissionSets;
import uk.ac.ox.it.ords.api.database.services.AccessImportService;
import uk.ac.ox.it.ords.api.database.services.CSVService;
import uk.ac.ox.it.ords.api.database.services.DatabaseAuditService;
import uk.ac.ox.it.ords.api.database.services.DatabaseRoleService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.ImportEmailService;
import uk.ac.ox.it.ords.api.database.services.SQLService;
import uk.ac.ox.it.ords.api.database.threads.ImportThread;
import uk.ac.ox.it.ords.security.model.DatabaseServer;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.services.PermissionsService;
import uk.ac.ox.it.ords.security.services.RestrictionsService;
import uk.ac.ox.it.ords.security.services.ServerConfigurationService;



public class DatabaseUploadServiceImpl extends DatabaseServiceImpl
		implements
			DatabaseUploadService {


	
	@Override
	public int createNewDatabaseFromFile(int logicalDatabaseId, File dbFile,
			String type, String server ) throws Exception {
		
		// get file size
		
		long dbFileSize = dbFile.length();
		
		// Check that user is allowed to create a database of that size
		long sizeAllowed = ((long)RestrictionsService.Factory.getInstance().getMaximumUploadSize()) * 1000000L;
		if ( dbFileSize > sizeAllowed ) {
			throw new UploadSizeException("Maximum file upload size: "+sizeAllowed);
		}
		
		//
		// Lookup the server
		//
		DatabaseServer databaseServer = ServerConfigurationService.Factory.getInstance().getDatabaseServer(server);
		if (databaseServer == null){
			throw new BadParameterException("Server not found");
		}
		

		OrdsPhysicalDatabase db = this.createPhysicalDatabase(
				logicalDatabaseId, type, dbFile.getName(), dbFile.length(),
				databaseServer);
		String databaseName = db.getDbConsumedName();
		DatabaseAuditService.Factory.getInstance().createImportRecord(db.getLogicalDatabaseId());
		DatabaseRoleService.Factory.getInstance().createInitialPermissions(db);

		if (type == null) {
			throw new BadParameterException("No type for uploaded file");
		}
		
		boolean runImportThread = false;
		
		if ( dbFileSize <= 100000000 ) {
			try {
				// less than a 100 MB just import the database directly
				// unless it's a sql file where a thread is used above 1 MB
				if ( type.equalsIgnoreCase("sql")) {
					if ( dbFileSize <= 1000000 ) {
						 SQLService service = SQLService.Factory.getInstance();
						 service.importSQLFileToDatabase(server, databaseName, dbFile, db.getPhysicalDatabaseId());
					}
					else {
						runImportThread = true;
					}
				}
				else if (type.equalsIgnoreCase("csv")) {
					CSVService service = CSVService.Factory.getInstance();
					service.newTableDataFromFile(server, databaseName, dbFile, true);
					this.setImportProgress(db.getPhysicalDatabaseId(), OrdsPhysicalDatabase.ImportType.FINISHED);
				}
				else {
					AccessImportService service = AccessImportService.Factory
							.getInstance();
					service.preflightImport(dbFile);
					service.createSchema(server, databaseName, dbFile);
					service.importData(server, databaseName, dbFile);
					this.setImportProgress(db.getPhysicalDatabaseId(), OrdsPhysicalDatabase.ImportType.FINISHED);
				}
			}
			catch(Exception e) {
				// remove the physical database and re-throw
				this.testDeleteDatabase(db.getPhysicalDatabaseId(), false);
				throw e;
				
			}
		}
		else {
			runImportThread = true;
		}
		if (runImportThread ) {
			// over 1 mb run through the exportThread
			ImportEmailService emailService = ImportEmailService.Factory.getInstance();
			emailService.setDatabaseName(dbFile.getName());
			String principle = (String) SecurityUtils.getSubject().getPrincipal();
			User u = this.getUserByPrincipal(principle);
			emailService.setEmail(u.getEmail());

			ImportThread importThread = new ImportThread(server,databaseName, dbFile, emailService, db.getPhysicalDatabaseId(), type);
		
			emailService.sendStartImportMessage();
			importThread.start();
		}
		return db.getPhysicalDatabaseId();
	}
	

//	@Override
//	public void init() throws Exception {
//		initializePermissions();
//	}
	

	@Override
	public String appendCSVToDatabase(int physicalDatabaseId, File csvFile,
			String server) throws Exception {
		long dbFileSize = csvFile.length();
		
		// Check that user is allowed to create a database of that size
		long sizeAllowed = RestrictionsService.Factory.getInstance().getMaximumUploadSize() * 1000;
		if ( dbFileSize > sizeAllowed ) {
			throw new UploadSizeException("Maximum file upload size: "+sizeAllowed);
		}

		OrdsPhysicalDatabase physicalDatabase = this
				.getPhysicalDatabaseFromID(physicalDatabaseId);
		String databaseName = physicalDatabase.getDbConsumedName();
		CSVService service = CSVService.Factory.getInstance();
		TableData data = service.newTableDataFromFile(server, databaseName, csvFile, true);
		return  data.tableName;
	}
	
	
	
	@Override
	public String appendCSVToDatabase(int physicalDatabaseId, File csvFile,
			String newTableName, String server) throws Exception {
		OrdsPhysicalDatabase physicalDatabase = this
				.getPhysicalDatabaseFromID(physicalDatabaseId);
		String databaseName = physicalDatabase.getDbConsumedName();
		CSVService service = CSVService.Factory.getInstance();
		TableData tableData = service.newTableDataFromFile(server, databaseName, newTableName, csvFile, true);
		return tableData.tableName;
	}
	
	
	@Override
	public void appendCSVToTable(int physicalDatabaseId, File csvFile,
			String tableName, String server, boolean header) throws Exception {
		OrdsPhysicalDatabase physicalDatabase = this.getPhysicalDatabaseFromID(physicalDatabaseId);
		String databaseName = physicalDatabase.getDbConsumedName();
		CSVService service = CSVService.Factory.getInstance();
		service.appendDataFromFile(server, databaseName, tableName, csvFile, header, true);
	}

	
	

	@Override
	public int importToExistingDatabase(int dbId, File sqlFile, String server)
			throws Exception {
		OrdsPhysicalDatabase physicalDatabase = this.getPhysicalDatabaseFromID(dbId);
		String databaseName = physicalDatabase.getDbConsumedName();
		SQLService service = SQLService.Factory.getInstance();
		service.importSQLFileToDatabase(server, databaseName, sqlFile, dbId);
		return physicalDatabase.getPhysicalDatabaseId();
	}
	
	@Override
	public void setImportProgress(int id, ImportType progress) throws Exception {

		OrdsPhysicalDatabase physicalDatabase = this.getPhysicalDatabaseFromID(id);
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			session.evict(physicalDatabase);
			physicalDatabase.setImportProgress(progress);
			session.update(physicalDatabase);
			session.flush();
			transaction.commit();
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}


	private OrdsPhysicalDatabase createPhysicalDatabase(int logicalDatabaseId,
			String databaseFileType, String fileName, long fileSize,
			DatabaseServer server) throws Exception {
		OrdsPhysicalDatabase db = new OrdsPhysicalDatabase();
		db.setLogicalDatabaseId(logicalDatabaseId);
		db.setEntityType(EntityType.MAIN);
		db.setImportProgress(ImportType.QUEUED);
		db.setDatabaseServer(server.getAlias());
		db.setFileName(fileName);
		db.setFileSize(fileSize);
		db.setFullPathToDirectory(System.getProperty("java.io.tmpdir")
				+ "/databases");
		db.setDatabaseType(databaseFileType);

		this.saveModelObject(db);
		if (db.getPhysicalDatabaseId() == 0) {
			throw new Exception(
					"Cannot retrieve database ID in newly created record");
		} else {
			//this.createOBDCUserRole(userName, password);
			String dbName = db.getDbConsumedName();
			String statement = String.format(
					"rollback transaction;create database %s;",
					dbName);

			this.runJDBCQuery(statement, null, server, null);
			String createSequence = "CREATE SEQUENCE ords_constraint_seq";
			this.runJDBCQuery(createSequence, null, server, dbName);

			return db;
		}
	}

//	private void initializePermissions() throws Exception {
//		PermissionsService service = PermissionsService.Factory.getInstance();
//		
//		
//		//
//		// "Anonymous" can view public databases
//		//
//		for (String permission : DatabasePermissionSets.getPermissionsForAnonymous() ) {
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("anonymous");
//			permissionObject.setPermission(permission);
//			service.createPermission(permissionObject);
//		}
//		
//		
//		
//		//
//		// Anyone with the "User" role can contribute to create databases
//		//
//		for (String permission : DatabasePermissionSets.getPermissionsForUser()) {
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("user");
//			permissionObject.setPermission(permission);
//			service.createPermission(permissionObject);
//		}
//
//
//		//
//		// Anyone with the "Administrator" role can create new full
//		// projects and upgrade projects to full, and update any
//		// user projects
//		//
//		for (String permission : DatabasePermissionSets
//				.getPermissionsForSysadmin()) {
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("administrator");
//			permissionObject.setPermission(permission);
//			service.createPermission(permissionObject);
//		}
//
//		//
//		// "Anonymous" can View public projects
//		//
//		for (String permission : DatabasePermissionSets
//				.getPermissionsForAnonymous()) {
//			Permission permissionObject = new Permission();
//			permissionObject.setRole("anonymous");
//			permissionObject.setPermission(permission);
//			service.createPermission(permissionObject);
//		}
//
//	}

	@Override
	public void testDeleteDatabase(int dbId, boolean staging) throws Exception {
		OrdsPhysicalDatabase database = getPhysicalDatabaseFromID(dbId);
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
		
		
		DatabaseServer databaseServer = ServerConfigurationService.Factory.getInstance().getDatabaseServer(database.getDatabaseServer());
		if (databaseServer == null){
			throw new Exception("Error locating server");
		}
		
		this.runJDBCQuery(statement, null, databaseServer, null);

	}




}
