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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.User;
import uk.ac.ox.it.ords.api.database.services.RestoreEmailService;
import uk.ac.ox.it.ords.api.database.services.SQLService;
import uk.ac.ox.it.ords.api.database.threads.RestoreThread;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;
import uk.ac.ox.it.ords.security.model.DatabaseServer;
import uk.ac.ox.it.ords.security.services.ServerConfigurationService;

public class SQLServicePostgresImpl extends DatabaseServiceImpl implements SQLService {
	private static Logger log = LoggerFactory.getLogger(SQLServicePostgresImpl.class);
	
	@Override
	public File exportSQLFileFromDatabase(int dbId) throws Exception {
		OrdsPhysicalDatabase database = this.getPhysicalDatabaseFromID(dbId);
		DatabaseServer server = ServerConfigurationService.Factory.getInstance().getDatabaseServer(database.getDatabaseServer());
		// create the file
		String databaseName = database.getDbConsumedName();
		File file = File.createTempFile("dump_" + databaseName, "sql");
		Properties properties = ConfigurationConverter.getProperties(MetaConfiguration.getConfiguration());
		String postgres_bin = "";
		if ( properties.containsKey("ords.postgresql.bin.path")) {
			postgres_bin = properties.getProperty("ords.postgresql.bin.path");
		}
		ProcessBuilder processBuilder = new ProcessBuilder(postgres_bin+"pg_dump", 
				"-f", 
				file.toString(), 
				"-v", "-o", "-h", 
				database.getDatabaseServer(), 
				"-U", 
				server.getUsername(), database.getDbConsumedName());
		
		processBuilder.environment().put("PGPASSWORD", server.getPassword());
		Process process = processBuilder.start();
		try {
			InputStream is = process.getInputStream();
			InputStreamReader reader = new InputStreamReader(is);
			BufferedReader buffer = new BufferedReader(reader);
			String line;
			while ((line = buffer.readLine()) != null ) {
				System.out.println(line);
				if (log.isDebugEnabled()) {
					log.debug(line);
				}
			}
		}
		catch ( Exception e ) {
			log.error("ERROR", e );
		}
		return file;
	}

	@Override
	public long importSQLFileToDatabase(String hostName,
			String databaseName, File sqlFile, int databaseId) throws Exception {
		DatabaseServer server = ServerConfigurationService.Factory.getInstance().getDatabaseServer(hostName);
		Subject s = SecurityUtils.getSubject();
		String principalName = s.getPrincipal().toString();
		User u = this.getUserByPrincipal(principalName);
		RestoreEmailService emailService = RestoreEmailService.Factory.getInstance();
		emailService.setEmail(u.getEmail());
		emailService.setDatabaseName(databaseName);
		
		RestoreThread rst = new RestoreThread(server.getHost(), databaseName, databaseId, server.getUsername(), server.getPassword(), sqlFile, emailService);
		
		// send an email to kick it off
		emailService.sendStartRestoreMessage();
		
		//Thread thread = new Thread(rst);
		rst.start();
		
		// return the thread id
		
		return rst.getId();
	}

}
