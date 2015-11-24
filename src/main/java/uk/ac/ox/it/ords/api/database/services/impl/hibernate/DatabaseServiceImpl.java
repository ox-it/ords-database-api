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
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;


public class DatabaseServiceImpl {
	
	protected static String ODBC_MASTER_PASSWORD_PROPERTY = "ords.odbc.masterpassword";
	
	private SessionFactory sessionFactory;

	private void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Class constructor - creates the session factory for accessing the ords
	 * database
	 */
	public DatabaseServiceImpl() {
		setSessionFactory(HibernateUtils.getSessionFactory());
	}

	/**
	 * Gets the session factory for accessing the ords database
	 * 
	 * @return
	 */
	public SessionFactory getOrdsDBSessionFactory() {
		return sessionFactory;
	}

	
	public String getODBCUser ( ) {
		String principalName = SecurityUtils.getSubject().getPrincipal()
				.toString();
		User u = this.getUserByPrincipal(principalName);
		return u.calculateOdbcUserForOrds();
		
	}
	
	
	public String getODBCPassword ( ) throws Exception {
		return MetaConfiguration.getConfiguration().getString(DatabaseServiceImpl.ODBC_MASTER_PASSWORD_PROPERTY);
	}
	
	
	
	   protected User getUserByPrincipal ( String principalName ) {
			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				
				@SuppressWarnings("unchecked")
				List<User> users = (List<User>) session.createCriteria(User.class).add(Restrictions.eq("principalName", principalName)).list();
				transaction.commit();
				if (users.size() == 1){
					return users.get(0);
				} 
				return null;
			} catch (Exception e) {
				session.getTransaction().rollback();
				throw e;
			}
			finally {
				session.close();
			}
	    }
	   
	   
		protected OrdsPhysicalDatabase getPhysicalDatabaseFromIDInstance ( int dbId, String instance){
			//EntityType dbType = OrdsPhysicalDatabase.EntityType.valueOf(instance
			//		.toUpperCase());
			//OrdsPhysicalDatabase database = this
			//		.getPhysicalDatabaseByLogicalDatabaseId(dbId, dbType);
			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				
				@SuppressWarnings("unchecked")
				List<OrdsPhysicalDatabase> users = (List<OrdsPhysicalDatabase>) session.createCriteria(OrdsPhysicalDatabase.class).add(Restrictions.eq("physicalDatabaseId", dbId)).list();
				transaction.commit();
				if (users.size() == 1){
					return users.get(0);
				} 
				return null;
			} catch (Exception e) {
				session.getTransaction().rollback();
				throw e;
			}
			finally {
				session.close();
			}
		}


}
