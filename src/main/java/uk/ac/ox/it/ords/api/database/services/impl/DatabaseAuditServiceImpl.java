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

package uk.ac.ox.it.ords.api.database.services.impl;

import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.UnavailableSecurityManagerException;

import uk.ac.ox.it.ords.api.database.services.DatabaseAuditService;
import uk.ac.ox.it.ords.security.model.Audit;
import uk.ac.ox.it.ords.security.services.AuditService;

public class DatabaseAuditServiceImpl implements
		DatabaseAuditService {
	
	static Logger log = Logger.getLogger(DatabaseAuditServiceImpl.class);
	
	private String getPrincipalName(){
		try {
			if (SecurityUtils.getSubject() == null || SecurityUtils.getSubject().getPrincipal() == null) return "Unauthenticated";
			return SecurityUtils.getSubject().getPrincipal().toString();
		} catch (UnavailableSecurityManagerException e) {
			log.warn("Audit being called with no valid security context. This is probably caused by being called from unit tests");
			return "Security Manager Not Configured";
		}
	}

	@Override
	public void createDatasetCreateRecord(int databaseId, int datasetId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.CREATE_VIEW.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		audit.setMessage("Created dataset " + datasetId + " for database "+ databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createDatasetDeleteRecord(int databaseId, int datasetId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.DELETE_VIEW.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		audit.setMessage("Deleted dataset " + datasetId + " for database "+ databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createDataQueryRecord(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.RUN_USER_QUERY.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createDataDeleteRecord(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.DELETE_DATA_ROW.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createDataInsertRecord(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.COMMIT_NEW_DATA.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		audit.setMessage("Data added");
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createDataChangeRecord(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.COMMIT_DATA_CHANGE.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage("Data modified");
		audit.setLogicalDatabaseId(databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}

	@Override
	public void createNotAuthRecord(String request) {
		Audit audit= new Audit();
		audit.setAuditType(Audit.AuditType.GENERIC_NOTAUTH.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage(request);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}
	
	@Override
	public void createNotAuthRecord(String request, int logicalDatabaseId) {
		Audit audit= new Audit();
		audit.setAuditType(Audit.AuditType.GENERIC_NOTAUTH.name());
		audit.setUserId(getPrincipalName());
		audit.setMessage(request);
		audit.setLogicalDatabaseId(logicalDatabaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}

	@Override
	public void createImportRecord(int databaseId) {
		Audit audit = new Audit();
		audit.setAuditType(Audit.AuditType.UPLOAD_DATABASE.name());
		audit.setUserId(getPrincipalName());
		audit.setLogicalDatabaseId(databaseId);
		audit.setMessage("Uploaded database "+ databaseId);
		AuditService.Factory.getInstance().createNewAudit(audit);
	}

	@Override
	public void createExportRecord(int databaseId) {
		// TODO
	}

}
