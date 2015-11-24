package uk.ac.ox.it.ords.api.database.conf;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AuthenticationDetails {
	private static Logger log = LoggerFactory.getLogger(AuthenticationDetails.class);
	private String ordsReadOnlyUser, ordsReadOnlyPassword;
	private String ordsUser, ordsPassword;
	private String datasetViewer, datasetViewerPassword;
	private String rootDbUser, rootDbPassword;
	private String ordsOdbcUserMasterPassword;
	
	

	public AuthenticationDetails() {
		try {
			String dbPropertiesLocation = MetaConfiguration
					.getConfigurationLocation("databaseProperties");
			PropertiesConfiguration prop = new PropertiesConfiguration(
					dbPropertiesLocation);

			//Properties prop = GeneralWebServicesUtils.readProperties(CommonVars.mainPropertiesFile);
			
			/**
			 * The user that can connect to and read/write from/to the ords database. Used for hibernate.
			 */
			ordsUser = prop.getProperty("user").toString();
			ordsPassword = prop.getProperty("password").toString();
			
			/**
			 * A user that can read from (but not write to) users databases. Used to gather statistics (number of records held by ords).
			 * TODO
			 * Currently thinking this is no longer needed - each user database is accessible via that user's ords ODBC role and there is
			 * always an owner so this could be used to collect statistics.
			 * Thus for now I shall deprecate this
			 */
			ordsReadOnlyUser = prop.getProperty("ordsReadOnlyUser").toString();
			ordsReadOnlyPassword = prop.getProperty("ordsReadOnlyPassword").toString();
			
			/**
			 * A read only user used to view public datasets
			 */
			datasetViewer = prop.getProperty("datasetViewer").toString();
			datasetViewerPassword = prop.getProperty("datasetViewerPassword").toString();
			
			/**
			 * A root user on the server that can create databases and set the public schema permissions
			 */
			rootDbUser = prop.getProperty("rootDbUser").toString();
			rootDbPassword = prop.getProperty("rootDbPassword").toString();
			
			/**
			 * When ords accesses data via a user command (e.g. the user logs in to a project and adds rows to a table in a 
			 * database), ords will do that via a special user linked to that of the user logged in. E.g., fred logs in to
			 * the project and performs the update: under the covers, ords will access the database with user fred_ords. 
			 * ordsOdbcUserMasterPassword provides the password for this access.
			 */
			ordsOdbcUserMasterPassword = prop.getProperty("ordsOdbcUserMasterPassword").toString();
            if ((ordsOdbcUserMasterPassword == null) || (ordsOdbcUserMasterPassword.length() == 0) ) {
                log.error("Unable to get odbc master password - defaulting");
                ordsOdbcUserMasterPassword = ordsPassword;
            }
		}
		catch (ConfigurationException e) {
			log.error("Unable to read main properties file " + CommonVars.mainPropertiesFile, e);
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
