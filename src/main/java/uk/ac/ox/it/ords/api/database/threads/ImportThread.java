package uk.ac.ox.it.ords.api.database.threads;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.AccessImportService;
import uk.ac.ox.it.ords.api.database.services.CSVService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.ImportEmailService;
import uk.ac.ox.it.ords.api.database.services.SQLService;

public class ImportThread extends Thread {
	private static Logger log = LoggerFactory.getLogger(ImportThread.class);
	private String server;
	private String databaseName;
	private File dbFile;
	private ImportEmailService emailService;
	private int databaseId;
	private String type;

	public ImportThread(String server, String databaseName, File dbFile,
			ImportEmailService emailService, int databaseId, String type) {
		this.server = server;
		this.databaseName = databaseName;
		this.dbFile = dbFile;
		this.emailService = emailService;
		this.databaseId = databaseId;
		this.type = type;
	}

	@Override
	public void run() {
		DatabaseUploadService uploadService = DatabaseUploadService.Factory
				.getInstance();
		try {
			uploadService.setImportProgress(databaseId,
					OrdsPhysicalDatabase.ImportType.IN_PROGRESS);
			if (type.equalsIgnoreCase("csv")) {
				CSVService service = CSVService.Factory.getInstance();
				service.newTableDataFromFile(server, databaseName, dbFile,
						true);
			} else if (type.equalsIgnoreCase("sql")) {
				SQLService service = SQLService.Factory.getInstance();
				service.importSQLFileToDatabase(server, databaseName, dbFile,
						databaseId);
			} else {
				AccessImportService service = AccessImportService.Factory
						.getInstance();
				service.preflightImport(dbFile);
				service.createSchema(server, databaseName, dbFile);
				service.importData(server, databaseName, dbFile);
			}
			emailService.sendImportSuccessfulMessage();
			uploadService.setImportProgress(databaseId,
					OrdsPhysicalDatabase.ImportType.FINISHED);
		} catch (Exception e) {
			log.error("ERROR", e);
			try {
				emailService.sendImportUnsuccessfulMessage(e.toString());
				uploadService.setImportProgress(databaseId,
						OrdsPhysicalDatabase.ImportType.FAILED);
			} catch (Exception e1) {
				log.error("ERROR", e1);
				e1.printStackTrace();
			}

		}
	}
}
