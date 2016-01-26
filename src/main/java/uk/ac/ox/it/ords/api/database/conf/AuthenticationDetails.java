package uk.ac.ox.it.ords.api.database.conf;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;



public class AuthenticationDetails {
	private static Logger log = LoggerFactory.getLogger(AuthenticationDetails.class);
	private String ordsReadOnlyUser, ordsReadOnlyPassword;
	private String ordsUser, ordsPassword;
	private String datasetViewer, datasetViewerPassword;
	private String rootDbUser, rootDbPassword;
	private String ordsOdbcUserMasterPassword;
	
	protected static String ODBC_MASTER_PASSWORD_PROPERTY = "ords.odbc.masterpassword";
	protected static String ORDS_DATABASE_NAME = "ords.database.name";
	protected static String ORDS_DATABASE_USER = "ords.database.user";
	protected static String ORDS_DATABASE_PASSWORD = "ords.database.password";
	protected static String ORDS_DATABASE_HOST = "ords.database.server.host";
	
	protected static String ORDS_READONLY_USER = "ords.database.readonlyuser";
	protected static String ORDS_READONLY_PASSWORD = "ords.database.readonlypassword";
	protected static String ORDS_DATASET_VIEWER = "ords.database.datasetviewer";
	protected static String ORDS_DATASET_PASSWORD = "ords.datasetviewerpassword";
	
	protected static String ORDS_DATABASE_ROOT_USER = "ords.database.rootdbuser";
	protected static String ORDS_DATABASE_ROOT_PASSWORD = "ords.database.rootdbpassword";
	
	/*
	 * ords.server.configuration=serverConfig.xml
ords.database.readonlyuser=ordsreadonly
ords.database.readonlypassword=
ords.database.datasetviewer=datasetViewer
ords.datasetviewerpassword=ords.database.name=ords2
ords.database.username=ords
ords.database.password=ords
ords.database.server.host=localhost
ords.database.rootdbuser=ords
ords.database.rootdbpassword=ords
ords.odbc.masterpassword=iU7*%fiXkls
ords.database.name=ords2
	 */
	
	

	public AuthenticationDetails() {
			//String dbPropertiesLocation = MetaConfiguration
			//		.getConfigurationLocation("databaseProperties");
			//PropertiesConfiguration prop = new PropertiesConfiguration(
			//		dbPropertiesLocation);
			
			Configuration properties = MetaConfiguration.getConfiguration();

			//Properties prop = GeneralWebServicesUtils.readProperties(CommonVars.mainPropertiesFile);
			
			/**
			 * The user that can connect to and read/write from/to the ords database. Used for hibernate.
			 */
			ordsUser = properties.getString(AuthenticationDetails.ORDS_DATABASE_USER);
			ordsPassword = properties.getString(AuthenticationDetails.ORDS_DATABASE_PASSWORD);
			
			/**
			 * A user that can read from (but not write to) users databases. Used to gather statistics (number of records held by ords).
			 * TODO
			 * Currently thinking this is no longer needed - each user database is accessible via that user's ords ODBC role and there is
			 * always an owner so this could be used to collect statistics.
			 * Thus for now I shall deprecate this
			 */
			ordsReadOnlyUser = properties.getString(AuthenticationDetails.ORDS_READONLY_USER);
			ordsReadOnlyPassword = properties.getString(AuthenticationDetails.ORDS_READONLY_PASSWORD);
			
			/**
			 * A read only user used to view public datasets
			 */
			datasetViewer = properties.getString(AuthenticationDetails.ORDS_DATASET_VIEWER);
			datasetViewerPassword = properties.getString(AuthenticationDetails.ORDS_DATASET_PASSWORD);
			
			/**
			 * A root user on the server that can create databases and set the public schema permissions
			 */
			rootDbUser = properties.getString(AuthenticationDetails.ORDS_DATABASE_ROOT_USER);
			rootDbPassword = properties.getString(AuthenticationDetails.ORDS_DATABASE_ROOT_PASSWORD);
			
			/**
			 * When ords accesses data via a user command (e.g. the user logs in to a project and adds rows to a table in a 
			 * database), ords will do that via a special user linked to that of the user logged in. E.g., fred logs in to
			 * the project and performs the update: under the covers, ords will access the database with user fred_ords. 
			 * ordsOdbcUserMasterPassword provides the password for this access.
			 */
			ordsOdbcUserMasterPassword = properties.getString(AuthenticationDetails.ODBC_MASTER_PASSWORD_PROPERTY);
            if ((ordsOdbcUserMasterPassword == null) || (ordsOdbcUserMasterPassword.length() == 0) ) {
                log.error("Unable to get odbc master password - defaulting");
                ordsOdbcUserMasterPassword = ordsPassword;
            }
	}

//	public String getOrdsReadOnlyUser() {
//		return ordsReadOnlyUser;
//	}
//
//	public String getOrdsReadOnlyPassword() {
//		return ordsReadOnlyPassword;
//	}

	public String getDatasetViewer() {
		return datasetViewer;
	}

	public String getDatasetViewerPassword() {
		return datasetViewerPassword;
	}

	public String getOrdsUser() {
		return ordsUser;
	}

	public String getOrdsPassword() {
		return ordsPassword;
	}

	public String getRootDbUser() {
		return rootDbUser;
	}
	
	public String getRootDbPassword() {
		return rootDbPassword;
	}
	
	public String getOrdsOdbcUserMasterPassword() {
		return ordsOdbcUserMasterPassword;
	}
}
