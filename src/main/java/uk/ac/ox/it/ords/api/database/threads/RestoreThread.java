/*
 * Copyright 2016 University of Oxford
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

package uk.ac.ox.it.ords.api.database.threads;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.RestoreEmailService;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;


public class RestoreThread extends Thread {
	private static Logger log = LoggerFactory.getLogger(RestoreThread.class);
	private String server;
	private String databaseName;
	private String databaseRole;
	private String databasePwd;
	private File dbFile;
	private RestoreEmailService emailService;
	private int databaseId;
	
	public RestoreThread ( String server, String databaseName, int databaseId, String role, String pwd, File dbFile, RestoreEmailService emailService) {
		this.server = server;
		this.databaseName = databaseName;
		this.databaseRole = role;
		this.databasePwd = pwd;
		this.dbFile = dbFile;
		this.emailService = emailService;
		this.databaseId = databaseId;
	}

	@Override
	public void run() {
		Properties properties = ConfigurationConverter.getProperties(MetaConfiguration.getConfiguration());
		String postgres_bin = "";
		if ( properties.containsKey("ords.postgresql.bin.path")) {
			postgres_bin = properties.getProperty("ords.postgresql.bin.path");
		}

		/*
		 * 		ProcessBuilder processBuilder = new ProcessBuilder(postgres_bin+"pg_dump", 
				"-f", 
				file.toString(), 
				"-v", "-o", "-h", 
				database.getDatabaseServer(), 
				"-U", 
				server.getUsername(), database.getDbConsumedName());

		 */
		ProcessBuilder processBuilder = new ProcessBuilder(postgres_bin+"psql", 
				"-d", 
				this.databaseName,
				"-h",
				this.server,
				"-U",
				this.databaseRole,
				"-f",
				this.dbFile.toString());
		processBuilder.environment().put("PGPASSWORD", this.databasePwd);
		DatabaseUploadService uploadService = DatabaseUploadService.Factory.getInstance();
		try {
			Process process = processBuilder.start();
			uploadService.setImportProgress(databaseId, OrdsPhysicalDatabase.ImportType.IN_PROGRESS);
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
			emailService.sendRestoreSuccessfulMessage();
			uploadService.setImportProgress(databaseId, OrdsPhysicalDatabase.ImportType.FINISHED);
		}
		catch ( Exception e ) {
			log.error("ERROR", e );
			try {
				emailService.sendRestoreUnsuccessfulMessage(e.toString());
				uploadService.setImportProgress(databaseId, OrdsPhysicalDatabase.ImportType.FAILED);
			} catch (Exception e1) {
				log.error("ERROR", e1);
				e1.printStackTrace();
			}
		}	
	}
}
