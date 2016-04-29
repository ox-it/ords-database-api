package uk.ac.ox.it.ords.api.database.queries;

//import java.io.BufferedReader;
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//import org.apache.commons.io.FileUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import uk.ac.ox.it.ords.api.database.data.DataRow;
//import uk.ac.ox.it.ords.api.database.data.DataTypeMap;
//import uk.ac.ox.it.ords.api.database.data.Row;
//import uk.ac.ox.it.ords.api.database.data.TableData;
//import uk.ac.ox.it.ords.api.database.exceptions.DBEnvironmentException;

/**
 * A key class, this contains the interface between ORDS and the underlying
 * database though various commands. I have tried to keep them
 * database-unspecific and there is a PostgresDBUtils that contains
 * Postgres-specific commands
 *
 * @author dave
 * @deprecated will remove - just keeping around for a bit in case this code is needed
 */
public class GeneralSQLQueries extends ORDSPostgresDB {

	public GeneralSQLQueries(String dbServer, String dbName) {
		super(dbServer, dbName);
		// TODO Auto-generated constructor stub
	}
	
	//private static Logger log = LoggerFactory.getLogger(GeneralSQLQueries.class);
	//private String dbCmdToBeRun;
	//private String dbErrorMessage;

	//@Override
	//public String getDbErrorMessage() {
	//	return dbErrorMessage;
	//}

//	public GeneralSQLQueries(String dbServer, String dbName) throws SQLException,
//			ClassNotFoundException, DBEnvironmentException {
//		super(dbServer, dbName);
//	}
    
//	/*
//	 * This is just a convenience constructor. It is a little naughty to introduce OrdsPhysicalDatabase here
//	 */
//	public DBUtils(OrdsPhysicalDatabase opd) throws SQLException,
//			ClassNotFoundException, DBEnvironmentException {
//		super(DBGatewayProjectNode.getHomeServerForOpd(opd), opd.getDbConsumedName());
//	}


	
//	public int getRecordCount(String table) throws SQLException, ClassNotFoundException {
//		int numberOfRecords = -1;
//
//		String command = "select count(*) from \"" + table + "\"";
//
//		if ((runDBQuery(command)) && (this.getTableData().rows.get(0) != null)) {
//			numberOfRecords = Integer.parseInt(getTableData().rows.get(0).cell.get(
//					"count").getValue());
//		}
//
//		return numberOfRecords;
//	}

//	/**
//	 * Takes the supplied file and parses it. The commands in the file will be
//	 * Takes the supplied file and parses it. The commands in the file will be
//	 * SQL commands, and this function will run each of them to create the
//	 * database.
//	 *
//	 * @param schemaFile
//	 * @return true if the file was successfully parsed and executed, else false
//	 * @throws IOException
//	 * @throws java.sql.SQLException
//	 * @throws ClassNotFoundException
//	 */
//	public boolean loadFileContentsIntoDatabase(File schemaFile) throws IOException, SQLException,
//			ClassNotFoundException {
//		if (log.isDebugEnabled()) {
//			log.debug("loadFileContentsIntoDatabase: "
//					+ schemaFile.getAbsolutePath());
//		}
//
//		String fullSchemaCommand = FileUtils.readFileToString(schemaFile);
//		if ((fullSchemaCommand == null)
//				|| (fullSchemaCommand.trim().length() == 0)) {
//			log.warn("No commands to run here. Assuming the table is empty!");
//			return true;
//		}
//		/*
//		 * Sometimes the commands might be of the form CREATE TABLE
//		 * tblAllHospitals ( HospitalID SERIAL, Place character varying(255),
//		 * primary key (HospitalID) ); CREATE TABLE tblApprenticeDoctor ( ...
//		 * 
//		 * and other times of the form
//		 * 
//		 * INSERT INTO tblPersonalIdentity VALUES(1,'Thame','PEC00 GMT
//		 * 1598',1598); INSERT INTO tblPersonalIdentity VALUES(2,'Brit Mar 03
//		 * 00:00:00 GMT 1685',1685);
//		 * 
//		 * So we can't necessarily split on \n or ;
//		 * 
//		 * Perhaps we can spilt on ;\n
//		 */
//		String[] commandlets = this
//				.normalizeCommands(fullSchemaCommand);
//		if (log.isDebugEnabled()) {
//			log.debug(String.format("There are %d commands to run",
//					commandlets.length));
//		}
//
//		for (String query : commandlets) {
//			if (!runDBQuery(query)) {
//				log.error("Problem running query " + query);
//				dbCmdToBeRun = query;
//				dbErrorMessage = getDbErrorMessage();
//				return false;
//			}
//		}
//
//		return true;
//	}
	
//	/**
//	 * Get rows of data from the table. This function will get a subset of the
//	 * table data.
//	 *
//	 * @param tableName
//	 *            the table whose data is to be returned
//	 * @param rowStart
//	 *            the number of the first row to return. In terms of the user,
//	 *            the row number starts at row 1, so for example they may
//	 *            display rows 1 to 30, meaning the first 30 rows of data. In
//	 *            terms of the database, however, the row number starts at 0, so
//	 *            rows 1 to 30 (from a user perspective) correspond to rows 0 to
//	 *            29 from a database perspective.
//	 * @param numberOfRowsRequired
//	 *            the number of rows to return. If zero, then all rows are
//	 *            returned.
//	 * @return table data or null if there has been an error or the table
//	 *         doesn't exist
//	 */
//	public TableData getTableDataForTable(String tableName) throws ClassNotFoundException, SQLException {
//		return getTableDataForTable(tableName, 1, 0);
//	}
//	public TableData getTableDataForTable(String tableName, int rowStart, int numberOfRowsRequired)
//			throws ClassNotFoundException, SQLException {
//		if (log.isDebugEnabled()) {
//			log.debug(String.format("getTableDataForTable(%s, %d, %d)", tableName, rowStart, numberOfRowsRequired));
//		}
//
//		return getTableDataForTable(tableName, rowStart, numberOfRowsRequired, null, true);
//	}	
	


	
//	/**
//	 * Dump the current database directly to file
//	 * @param file
//	 * @param dbIP
//	 * @return true if the database has been dumped successfully
//	 * @throws IOException
//	 */
//	public boolean dumpDatabaseDirect(File file, String dbIP) throws IOException {
//		boolean ret = false;
//
//		// http://stackoverflow.com/questions/55709/streaming-large-files-in-a-java-servlet
//		// http://stackoverflow.com/questions/685271/using-servletoutputstream-to-write-very-large-files-in-a-java-servlet-without-me
//		log.debug("dumpDatabaseDirect");
//		try {
//			Process p;
//			ProcessBuilder pb;
//
//			// java.io.File file = new java.io.File(outputDir);
//			/* We test if the path to our programs exists */
//			if (file.exists()) {
//				if (dbIP == null) {
//					log.error("DBIP NULL");
//					return false;
//				}
//
//				pb = new ProcessBuilder("pg_dump", "-f", file.toString(), 
//						"-v", "-o", "-h", dbIP, "-U", getCurrentDbUser(), getCurrentDbName());
//				if (log.isDebugEnabled()) {
//					log.debug(String.format("About to run the command pg_dump -f %s -v -o -h %s -U %s %s",
//							file.toString(), dbIP, getCurrentDbUser(), getCurrentDbName()));
//				}
//				pb.environment().put("PGPASSWORD", getCurrentDbPw());
//				pb.redirectErrorStream(true);
//				p = pb.start();
//				try {
//					InputStream is = p.getInputStream();
//					InputStreamReader isr = new InputStreamReader(is);
//					BufferedReader br = new BufferedReader(isr);
//					String ll;
//					log.debug("Logging output ...");
//					while ((ll = br.readLine()) != null) {
//						System.out.println(ll);
//						if (log.isDebugEnabled()) {
//							log.debug(ll);
//						}
//					}
//					ret = true;
//				}
//				catch (IOException e) {
//					log.error("ERROR ", e);
//				}
//			}
//
//		}
//		catch (IOException e) {
//			log.error("Could not invoke browser, command=");
//			log.error("Caught: " + e.getMessage());
//		}
//
//		log.debug("dumpDatabaseDirect:return");
//
//		return ret;
//	}
	
	
//    /**
//     * This will take a String of commands and return them as an array of individual commands.
//     * It is assumed that each command starts on a new line but that commands may (or may not)
//     * span many lines.
//     * @param command the command(s) to process
//     * @return an array of corresponding commands, one per array object. Null in, null out.
//     */
//    private  String[] normalizeCommands(String command) {
//        log.debug("normalizeCommands");
//        if (command == null) {
//            log.error("Null input");
//            return null;
//        }
//        List<String> result = new ArrayList<String>();
//        
//        String [] tempArray = command.split("\n");
//        String individualCommand = "";
//        for (String s : tempArray) {
//            String s2 = s.trim();
//            individualCommand += s2;
//            if (s2.endsWith(";")) {
//                // New command required
//                result.add(individualCommand);
//                individualCommand = "";
//            }
//        }
//        
//        
//        return result.toArray(new String[result.size()]);
//    }

}
