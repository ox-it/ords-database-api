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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;
import org.hibernate.Criteria;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Expression;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.SimpleExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.model.OrdsDB;
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

	// public String getODBCUser ( ) throws ConfigurationException {
	// Subject s = SecurityUtils.getSubject();
	// if ( s.isAuthenticated()) {
	// String principalName = s.getPrincipal().toString();
	// User u = this.getUserByPrincipal(principalName);
	// return u.calculateOdbcUserForOrds();
	// }
	// else {
	// return getORDSDatabaseUser();
	// }
	//
	// }
	//
	//
	// public String getODBCPassword ( ) throws Exception {
	// Subject s = SecurityUtils.getSubject();
	// if ( s.isAuthenticated() ) {
	// return
	// MetaConfiguration.getConfiguration().getString(DatabaseServiceImpl.ODBC_MASTER_PASSWORD_PROPERTY);
	// }
	// else {
	// return this.getORDSDatabasePassword();
	// }
	// }
	//

	public String getORDSDatabaseUser() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_USER);
	}

	public String getORDSDatabasePassword() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_PASSWORD);
	}

	public String getORDSDatabaseName() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_NAME);
	}

	public String getORDSDatabaseHost() throws ConfigurationException {
		return MetaConfiguration.getConfiguration().getString(
				DatabaseServiceImpl.ORDS_DATABASE_HOST);
	}

	protected User getUserByPrincipal(String principalName) {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();

			@SuppressWarnings("unchecked")
			List<User> users = (List<User>) session.createCriteria(User.class)
					.add(Restrictions.eq("principalName", principalName))
					.list();
			transaction.commit();
			if (users.size() == 1) {
				return users.get(0);
			}
			return null;
		} catch (Exception e) {
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}

	protected OrdsDB getLogicalDatabaseFromID(int id) {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("logicalDatabaseId", id));
		return this.getModelObject(exprs, OrdsDB.class);

	}

	protected OrdsPhysicalDatabase getPhysicalDatabaseFromID(int dbId) {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("physicalDatabaseId", dbId));
		// exprs.add(Restrictions.eq("instance", instance));
		return this.getModelObject(exprs, OrdsPhysicalDatabase.class);

		// Session session = this.getOrdsDBSessionFactory().openSession();
		// try {
		// Transaction transaction = session.beginTransaction();
		//
		// @SuppressWarnings("unchecked")
		// List<OrdsPhysicalDatabase> users = (List<OrdsPhysicalDatabase>)
		// session.createCriteria(OrdsPhysicalDatabase.class).add(Restrictions.eq("physicalDatabaseId",
		// dbId)).list();
		// transaction.commit();
		// if (users.size() == 1){
		// return users.get(0);
		// }
		// return null;
		// } catch (Exception e) {
		// session.getTransaction().rollback();
		// throw e;
		// }
		// finally {
		// session.close();
		// }
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

	protected void updateModelObject(Object objectToUpdate) throws Exception {
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

	protected <T> T getModelObject(List<SimpleExpression> restrictions,
			Class<T> cls) {
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			Criteria c = session.createCriteria(cls);
			for (SimpleExpression exp : restrictions) {
				c.add(exp);
			}
			@SuppressWarnings("unchecked")
			List<T> objects = (List<T>) c.list();
			transaction.commit();
			if (objects.size() == 1) {
				return objects.get(0);
			}
			return null;
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

	@SuppressWarnings("rawtypes")
	protected List runSQLQuery(String query) {
		Session session;
		session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			SQLQuery sqlQuery = session.createSQLQuery(query);
			List results = sqlQuery.list();
			transaction.commit();
			return results;

		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}

	public boolean checkDatabaseExists(String databaseName) throws Exception {
		String sql = "SELECT COUNT(*) as count from pg_database WHERE datname = ?";
		List<Object> parameters = this.createParameterList(databaseName);
		return this.runCountSql(sql, parameters, null, null, null, null) == 1;
	}

	protected List<Object> createParameterList(Object... args) {
		ArrayList<Object> parameters = new ArrayList<Object>();
		for (Object p : args) {
			parameters.add(p);
		}
		return parameters;
	}

	private int runCountSql(String sql, List<Object> parameters, String dbName,
			String databaseServer, String username, String password)
			throws Exception {
		CachedRowSet result = this.runJDBCQuery(sql, parameters,
				databaseServer, dbName);
		try {
			// If count is 1, then a table with the given name was found
			while (result.next()) {
				return result.getInt("count");
			}
		} finally {
			if (result != null)
				result.close();
		}
		return 0;

	}

	protected String getTerminateStatement(String databaseName)
			throws Exception {
		boolean above9_2 = isPostgresVersionAbove9_2();
		String command;
		if (above9_2) {
			log.info("Postgres version is 9.2 or later");
			command = String
					.format("select pg_terminate_backend(pid) from pg_stat_activity where datname = '%s' AND pid <> pg_backend_pid()",
							databaseName);
		} else {
			log.info("Postgres version is earlier than 9.2");
			command = String
					.format("select pg_terminate_backend(procpid) from pg_stat_activity where datname = '%s' AND procpid <> pg_backend_pid()",
							databaseName);
		}
		return command;

	}

	private String[] getPostgresVersionArray() throws Exception {

		String version = (String) this.singleResultQuery("SELECT version()");

		String[] versionArray = null;
		String[] tempVersionArray = null;
		tempVersionArray = version.split(" ");
		version = tempVersionArray[1];
		versionArray = version.split("\\.");

		return versionArray;
	}

	protected boolean isPostgresVersionAbove9_2() throws Exception {
		String[] versionArray = getPostgresVersionArray();
		boolean above = false;
		if (versionArray != null) {
			try {
				int majorVersionNumber = Integer.parseInt(versionArray[0]);
				int minorVersionNumber = Integer.parseInt(versionArray[1]);
				if (majorVersionNumber >= 9) {
					if (minorVersionNumber >= 2) {
						above = true;
					}
				}
			} catch (NumberFormatException e) {
				log.error("Unable to get Postgres version");
			}
		}

		return above;
	}

	protected void createOBDCUserRole(String username, String password)
			throws Exception {

		// check if role exists already
		String sql = String.format("SELECT 1 FROM pg_roles WHERE rolname='%s'",
				username);
		@SuppressWarnings("rawtypes")
		List r = this.runSQLQuery(sql);
		if (r.size() == 0) {
			// role doesn't exist
			String command = String
					.format("create role \"%s\" nosuperuser login createdb inherit nocreaterole password '%s' valid until '2045-01-01'",
							username, password);
			this.runSQLStatementOnOrdsDB(command);
		}
	}
	/**
	 * Mimicks the postgres function, surrounding a table or column name in
	 * quotes, escaping existing quotes by doubling them.
	 * 
	 * @param ident
	 *            The table, column or other object name.
	 * @return
	 */
	protected String quote_ident(String ident) {
		return "\"" + ident.replace("\"", "\"\"") + "\"";
	}

	/**
	 * Mimicks the postgres function, surrounding a string in quotes, escaping
	 * existing quotes by doubling them.
	 * 
	 * @param literal
	 * @return
	 */
	protected String quote_literal(String literal) {
		if (literal == null) {
			return literal;
		}
		return "'" + literal.replace("'", "''") + "'";
	}

	private Connection createConnection(String server, String databaseName,
			boolean readOnly) throws SQLException, ConfigurationException {
		Connection connection = null;
		Properties connectionProperties = new Properties();
		if (server == null) {
			server = this.getORDSDatabaseHost();
		}
		connectionProperties.put("user", this.getORDSDatabaseUser());
		connectionProperties.put("password", this.getORDSDatabasePassword());
		if (databaseName == null) {
			databaseName = this.getORDSDatabaseName();
		}
		String connectionURL = "jdbc:postgresql://" + server + "/"
				+ databaseName;
		connection = DriverManager.getConnection(connectionURL,
				connectionProperties);
		connection.setReadOnly(readOnly);
		return connection;
	}

	private CachedRowSet runPreparedStatement(String query,
			List<Object> parameters, Connection connection) throws SQLException {
		PreparedStatement preparedStatement = null;
		try {
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
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	protected CachedRowSet runReadOnlyJDBCQuery(String query,
			List<Object> parameters, String server, String databaseName)
			throws Exception {
		Connection connection = null;
		try {
			connection = createConnection(server, databaseName, true);
			return runPreparedStatement(query, parameters, connection);

		} catch (SQLException e) {
			log.error("Error with this command", e);
			log.error("Query:" + query);
			throw e;
		} finally {
			if (connection != null) {
				connection.close();
			}
		}

	}

	protected CachedRowSet runJDBCQuery(String query, List<Object> parameters,
			String server, String databaseName) throws Exception {
		Connection connection = null;
		try {
			connection = createConnection(server, databaseName, false);
			return runPreparedStatement(query, parameters, connection);

		} catch (SQLException e) {
			log.error("Error with this command", e);
			log.error("Query:" + query);
			throw e;
		} finally {
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
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		} finally {
			session.close();
		}
	}

}
