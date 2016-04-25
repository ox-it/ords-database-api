package uk.ac.ox.it.ords.api.database.conf;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;



public class AuthenticationDetails {

	private String ordsUser, ordsPassword;
	
	protected static String ORDS_DATABASE_NAME = "ords.database.name";
	protected static String ORDS_DATABASE_USER = "ords.database.user";
	protected static String ORDS_DATABASE_PASSWORD = "ords.database.password";
	protected static String ORDS_DATABASE_HOST = "ords.database.server.host";

	public AuthenticationDetails() {

			Configuration properties = MetaConfiguration.getConfiguration();
			
			/**
			 * The user that can connect to and read/write from/to the ords database. Used for hibernate.
			 */
			ordsUser = properties.getString(AuthenticationDetails.ORDS_DATABASE_USER);
			ordsPassword = properties.getString(AuthenticationDetails.ORDS_DATABASE_PASSWORD);

	}

	public String getOrdsUser() {
		return ordsUser;
	}

	public String getOrdsPassword() {
		return ordsPassword;
	}
}
