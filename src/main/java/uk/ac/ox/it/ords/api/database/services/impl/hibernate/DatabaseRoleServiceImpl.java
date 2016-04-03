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

import java.util.List;

import org.apache.shiro.SecurityUtils;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissionSets;
import uk.ac.ox.it.ords.api.database.server.ValidationException;
import uk.ac.ox.it.ords.api.database.services.DatabaseRoleService;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;

public class DatabaseRoleServiceImpl
		implements
			DatabaseRoleService {

	private static Logger log = LoggerFactory.getLogger(DatabaseRoleServiceImpl.class);
	protected SessionFactory sessionFactory;

	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	public DatabaseRoleServiceImpl() {
		setSessionFactory (HibernateUtils.getSessionFactory());
	}
	
	
	@Override
	public void createInitialPermissions(OrdsPhysicalDatabase database) throws Exception {
		Session session = this.sessionFactory.openSession();
		
		try {
			session.beginTransaction();
			//
			// Assign the principal to the owner role
			//
			UserRole owner = new UserRole();
			owner.setPrincipalName(SecurityUtils.getSubject().getPrincipal().toString());
			owner.setRole(getPrivateUserRole("databaseowner", database.getLogicalDatabaseId()));
			session.save(owner);
			session.getTransaction().commit();
			//
			// Create the permissions for roles associated with the project
			//
			createPermissionsForDatabase(database.getLogicalDatabaseId());

		} catch (HibernateException e) {
			log.error("Error creating Project", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot create project",e);
		}
		finally {
			session.close();
		}

	}
	
	@Override
	public String getPrivateUserRole(String role, int projectId) {
		if (role.contains("_")) return role;
		return role+"_"+projectId;
	}

	/**
	 * Each project has a set of roles and permissions
	 * associated with it.
	 * 
	 * By default these are:
	 * 
	 *   owner_{projectId}
	 *   contributor_{projectId}
	 *   viewer_{projectId}
	 *   
	 * @param projectId
	 * @throws Exception 
	 */
	private void createPermissionsForDatabase(int dbId) throws Exception{
		//
		// Owner
		//
		String ownerRole = "databaseowner_"+dbId;
		for (String permission : DatabasePermissionSets.getPermissionsForOwner(dbId)){
			createPermission(ownerRole, permission);			
		}

		//
		// Contributor
		//
		String contributorRole = "databasecontributor_"+dbId;
		for (String permission : DatabasePermissionSets.getPermissionsForContributor(dbId)){
			createPermission(contributorRole, permission);			
		}

		//
		// Viewer
		//
		String viewerRole = "databaseviewer_"+dbId;
		for (String permission : DatabasePermissionSets.getPermissionsForViewer(dbId)){
			createPermission(viewerRole, permission);			
		}
	}

	/**
	 * @param role
	 * @param permissionString
	 * @throws Exception
	 */
	protected void createPermission(String role, String permissionString) throws Exception{
		Session session = this.sessionFactory.openSession();
		try {
			session.beginTransaction();
			Permission permission = new Permission();
			permission.setRole(role);
			permission.setPermission(permissionString);
			session.save(permission);
			session.getTransaction().commit();
		} catch (Exception e) {
			log.error("Error creating permission", e);
			session.getTransaction().rollback();
			throw new Exception("Cannot create permission",e);
		}
		finally {
			session.close();
		}

	}

	@Override
	public String getPublicUserRole(String role) {
		// TODO Auto-generated method stub
		return null;
	}

}
