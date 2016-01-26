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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.shiro.SecurityUtils;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;


public class DatabaseServiceImpl {

	Logger log = LoggerFactory.getLogger(DatabaseServiceImpl.class);

	protected static String ODBC_MASTER_PASSWORD_PROPERTY = "ords.odbc.masterpassword";
	protected static String ORDS_DATABASE_NAME = "ords.database.name";
	protected static String ORDS_DATABASE_USER = "ords.database.user";
	protected static String ORDS_DATABASE_PASSWORD = "ords.database.password";
	protected static String ORDS_DATABASE_HOST = "ords.database.server.host";
	
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
	
	
	public String getORDSDatabaseUser() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_USER);
	}
	
	
	public String getORDSDatabasePassword()  throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_PASSWORD);
	}

	public String getORDSDatabaseName() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_NAME);
	}
	
	
	public String getORDSDatabaseHost()  throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_HOST);
	}

	public String getDatabaseServer( int dbId, String instance ) throws Exception {
		OrdsPhysicalDatabase pdb = this.getPhysicalDatabaseFromIDInstance(dbId, instance);
		return pdb.getDatabaseServer();
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
		
		
		protected void saveModelObject(Object objectToSave) throws Exception {
			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				session.save(objectToSave);
				transaction.commit();
			} catch (Exception e) {
				log.debug(e.getMessage());
				session.getTransaction().rollback();
				throw e;
			} finally {
				session.close();
			}
		}
		
		
		protected void updateModelObject(Object objectToUpdate ) throws Exception {
			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				session.update(objectToUpdate);
				transaction.commit();
			} catch (Exception e) {
				log.debug(e.getMessage());
				session.getTransaction().rollback();
				throw e;
			} finally {
				session.close();
			}
		}

		protected void removeModelObject(Object objectToRemove) throws Exception {
			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				session.delete(objectToRemove);
				transaction.commit();
			} catch (Exception e) {
				log.debug(e.getMessage());
				session.getTransaction().rollback();
				throw e;
			} finally {
				session.close();
			}
		}
		
		
		protected String calculateStagingName(String dbName) {
			return dbName + "_staging";
		}

		
		protected Object singleResultQuery(String query) throws Exception {

			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				SQLQuery sqlQuery = session.createSQLQuery(query);
				Object result = sqlQuery.uniqueResult();
				transaction.commit();
				return result;
			} catch (Exception e) {
				log.debug(e.getMessage());
				session.getTransaction().rollback();
				throw e;
			} finally {
				session.close();
			}
		}

		
		protected CachedRowSet runJDBCQuery(String query, List<Object> parameters,
				String server, String databaseName) throws Exception {
			Connection connection = null;
			Properties connectionProperties = new Properties();
			PreparedStatement preparedStatement = null;
			if ( server != null && databaseName != null ){
				
				String userName = this.getODBCUser();
				String password = this.getODBCPassword();
				connectionProperties.put("user", userName);
				connectionProperties.put("password", password);
			}
			else {
				// get the ords database configuration
				//Configuration config = MetaConfiguration.getConfiguration();
				connectionProperties.put("user", this.getORDSDatabaseUser());
				connectionProperties.put("password", this.getORDSDatabasePassword());
				if ( server == null ) {
					server = this.getORDSDatabaseHost();
				}
				if (databaseName == null ) {
					databaseName = this.getORDSDatabaseName();
				}
			}
			String connectionURL = "jdbc:postgresql://" + server + "/"
					+ databaseName;
			try {
				connection = DriverManager.getConnection(connectionURL,
						connectionProperties);
				preparedStatement = connection.prepareStatement(query);
				if (parameters != null) {
					int paramCount = 1;
					for (Object parameter : parameters) {
						@SuppressWarnings("rawtypes")
						Class type = parameter.getClass();
						if (type.equals(String.class)) {
							preparedStatement.setString(paramCount,
									(String) parameter);
						}
						if (type.equals(Integer.class)) {
							preparedStatement.setInt(paramCount,
									(Integer) parameter);
						}
						paramCount++;
					}

				}
				if (query.toLowerCase().startsWith("select")) {
					ResultSet result = preparedStatement.executeQuery();
					CachedRowSet rowSet = RowSetProvider.newFactory()
							.createCachedRowSet();
					rowSet.populate(result);
					log.debug("prepareAndExecuteStatement:return result");
					return rowSet;
				} else {
					preparedStatement.execute();
					log.debug("prepareAndExecuteStatement:return null");
					return null;
				}

			} catch (SQLException e) {
				log.error("Error with this command", e);
				log.error("Query:" + query);
				throw e;
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (connection != null) {
					connection.close();
				}
			}

		}
		
		protected void runSQLStatementOnOrdsDB(String statement) {
			Session session = this.getOrdsDBSessionFactory().openSession();
			try {
				Transaction transaction = session.beginTransaction();
				SQLQuery query = session.createSQLQuery(statement);
				query.executeUpdate();
				transaction.commit();
			}
			catch (Exception e) {
				log.debug(e.getMessage());
				session.getTransaction().rollback();
				throw e;
			}
			finally {
				session.close();
			}
		}




}
