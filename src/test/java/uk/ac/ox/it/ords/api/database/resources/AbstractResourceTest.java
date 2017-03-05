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
package uk.ac.ox.it.ords.api.database.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cxf.endpoint.Server;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.lifecycle.ResourceProvider;
import org.apache.cxf.jaxrs.lifecycle.SingletonResourceProvider;
import org.apache.cxf.transport.local.LocalConduit;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.Factory;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissionSets;
import uk.ac.ox.it.ords.api.database.services.DatabaseAuditService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.impl.hibernate.HibernateUtils;
import uk.ac.ox.it.ords.security.AbstractShiroTest;
import uk.ac.ox.it.ords.security.model.Permission;
import uk.ac.ox.it.ords.security.model.UserRole;




public class AbstractResourceTest extends AbstractShiroTest {

	protected final static String ENDPOINT_ADDRESS = "local://database-api";
	protected static Server server;
	protected static ArrayList<DatabaseReference> databases;
	
	protected static void startServer() throws Exception {

	}
	
	public WebClient getClient(boolean json){
		List<Object> providers = new ArrayList<Object>();
		providers.add(new JacksonJsonProvider());
		WebClient client = WebClient.create(ENDPOINT_ADDRESS, providers);
		if ( json ) client.type("application/json");
		client.accept("application/json");
		WebClient.getConfig(client).getRequestContext().put(LocalConduit.DIRECT_DISPATCH, Boolean.TRUE);
		return client;
	}
	
	
	public static void createTestUsersAndRoles() throws Exception{
		
		//
		// Set up the database
		// Before we setup our database we need to make sure that ords-security-common
		// hibernate session is set up so internal calls to it won't stomp over our settings
		
		DatabaseAuditService.Factory.getInstance().createNotAuthRecord("dummy");
		//
		// Set up the test users and their permissions
		//
		Session session = HibernateUtils.getSessionFactory().getCurrentSession();
		Transaction transaction = session.beginTransaction();
		
		//
		// Clear out anything there already
		//
		session.createSQLQuery("truncate userrole, permissions, ordsuser, ordsphysicaldatabase").executeUpdate();
		transaction.commit();
		
		
		//
		// Add our test permissions
		//
		
		
		//DatabaseUploadService.Factory.getInstance().init();

		session = HibernateUtils.getSessionFactory().getCurrentSession();
		transaction = session.beginTransaction();
		
		//
		// Anyone with the "User" role can contribute to projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForUser()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("user");
			permissionObject.setPermission(permission);
			session.save(permissionObject);
		}
		
		//
		// Anyone with the "LocalUser" role can create new trial projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForLocalUser()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("localuser");
			permissionObject.setPermission(permission);
			session.save(permissionObject);
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
			session.save(permissionObject);
		}

		//
		// "Anonymous" can View public projects
		//
		for (String permission : DatabasePermissionSets.getPermissionsForAnonymous()){
			Permission permissionObject = new Permission();
			permissionObject.setRole("anonymous");
			permissionObject.setPermission(permission);
			session.save(permissionObject);
		}
	
		//
		// Add test users to roles
		//
		UserRole admin = new UserRole();
		admin.setPrincipalName("admin");
		admin.setRole("administrator");
		session.save(admin);
		
		UserRole pingu = new UserRole();
		pingu.setPrincipalName("ivor");
		pingu.setRole("localuser");
		session.save(pingu);
		
		UserRole pingu2 = new UserRole();
		pingu2.setPrincipalName("ivor");
		pingu2.setRole("premiumuser");
		session.save(pingu2);
		
		UserRole pingo = new UserRole();
		pingo.setPrincipalName("jack@nowhere.co");
		pingo.setRole("user");
		session.save(pingo);
		
		UserRole anonymous = new UserRole();
		anonymous.setPrincipalName("anonymous");
		anonymous.setRole("anonymous");
		session.save(anonymous);
		
//		//
//		// Create equivalent ords users
//		//
//		User adminOrds = new User();
//		adminOrds.setName("admin");
//		adminOrds.setPrincipalName("admin@nowhere.co");
//		adminOrds.setEmail("admin@nowhere.co");
//		adminOrds.setOdbcUser(adminOrds.getEmail().replace("@", "").replace(".", ""));
//		adminOrds.setStatus(User.AccountStatus.VERIFIED.toString());
//		adminOrds.setVerificationUuid(UUID.randomUUID().toString());
//		session.save(adminOrds);
//
//		User anonymousOrds = new User();
//		anonymousOrds.setName("anonymous");
//		anonymousOrds.setPrincipalName("anonymous@nowhere.co");
//		anonymousOrds.setEmail("anonymous@nowhere.co");
//		anonymousOrds.setOdbcUser(anonymousOrds.getEmail().replace("@", "").replace(".", ""));
//		anonymousOrds.setStatus(User.AccountStatus.VERIFIED.toString());
//		anonymousOrds.setVerificationUuid(UUID.randomUUID().toString());
//		session.save(anonymousOrds);
//
//		
		//
		// We only need one user now for the dataset tests
		//
		User jackOrds = new User();
		jackOrds.setName("jack");
		jackOrds.setPrincipalName("jack@nowhere.co");
		jackOrds.setEmail("jack@nowhere.co");
		jackOrds.setOdbcUser(jackOrds.getEmail().replace("@", "").replace(".", ""));
		jackOrds.setStatus(User.AccountStatus.VERIFIED.toString());
		jackOrds.setVerificationUuid(UUID.randomUUID().toString());
		session.save(jackOrds);
//
//		User ivorOrds = new User();
//		ivorOrds.setName("ivor");
//		ivorOrds.setPrincipalName("ivor@nowhere.co");
//		ivorOrds.setEmail("ivor@nowhere.co");
//		ivorOrds.setOdbcUser(ivorOrds.getEmail().replace("@", "").replace(".", ""));
//		ivorOrds.setStatus(User.AccountStatus.VERIFIED.toString());
//		ivorOrds.setVerificationUuid(UUID.randomUUID().toString());
//		session.save(ivorOrds);

		//
		// Commit our changes
		//
		transaction.commit();
		HibernateUtils.closeSession();
	}
	
	// convenience and sanity
	public void loginBasicUser() {
		loginUsingSSO("jack@nowhere.co", "jack@nowhere.co");
	}
	
	public void loginAnonymous() {
		loginUsingSSO("anon", "anon");
	}
	
	public void loginUsingPremiumUser() {
		loginUsingSSO("ivor", "ivor");
	}
	
	public void loginUsingAdmin() {
		loginUsingSSO("admin", "admin");
	}
	

	/**
	 * Configure Shiro and start the server
	 * @throws Exception
	 */
	@BeforeClass
	public static void initialize() throws Exception {
	
		//
		// Set up roles
		//
		createTestUsersAndRoles();
		
		//
		// This is for unit testing only and uses the test.shiro.ini configuration
		//
		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:test.shiro.ini");
		SecurityManager securityManager = factory.getInstance();
		SecurityUtils.setSecurityManager(securityManager);
		
		//
		// Create an embedded server with JSON processing
		//

		
		//
		// Create an embedded server with JSON processing
		//
		JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();
		
		ArrayList<Object> providers = new ArrayList<Object>();
		providers.add(new JacksonJsonProvider());
		//providers.add(new UnrecognizedPropertyExceptionMapper());
		//providers.add(new ValidationExceptionMapper());
		sf.setProviders(providers);
		
		//
		// Add our REST resources to the server
		//
		ArrayList<ResourceProvider> resources = new ArrayList<ResourceProvider>();
		
		//
		// Add our REST resources to the server
		//
		resources.add(new SingletonResourceProvider(new Database(), true));
		
		sf.setResourceProviders(resources);
		
		//
		// Start the server at the endpoint
		//
		sf.setAddress(ENDPOINT_ADDRESS);
		server = sf.create(); 
		databases = new ArrayList<DatabaseReference>();
		startServer();
	}

	@AfterClass
	public static void destroy() throws Exception {
		DatabaseUploadService dus = DatabaseUploadService.Factory.getInstance();
		for ( DatabaseReference db: databases ) {
			dus.testDeleteDatabase(db.id, db.staging);
		}
		server.stop();
		server.destroy();
	}

	@After
	public void logout() throws Exception{
		SecurityUtils.getSubject().logout();
	}



}
