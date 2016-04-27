package uk.ac.ox.it.ords.api.database.queries;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.exceptions.DBEnvironmentException;

/**
 * A key class, this contains the interface between ORDS and the underlying
 * database though various commands. I have tried to keep them
 * database-unspecific and there is a PostgresDBUtils that contains
 * Postgres-specific commands
 *
 * @author dave
 *
 */
public class GeneralSQLQueries extends ORDSPostgresDB {
	private static Logger log = LoggerFactory.getLogger(GeneralSQLQueries.class);
	private String dbCmdToBeRun;
	private String dbErrorMessage;


	public String getDbCmdToBeRun() {
		return dbCmdToBeRun;
	}

	@Override
	public String getDbErrorMessage() {
		return dbErrorMessage;
	}


	public GeneralSQLQueries(String dbServer, String dbName) throws SQLException,
			ClassNotFoundException, DBEnvironmentException {
		super(dbServer, dbName);
	}
    
//	/*
//	 * This is just a convenience constructor. It is a little naughty to introduce OrdsPhysicalDatabase here
//	 */
//	public DBUtils(OrdsPhysicalDatabase opd) throws SQLException,
//			ClassNotFoundException, DBEnvironmentException {
//		super(DBGatewayProjectNode.getHomeServerForOpd(opd), opd.getDbConsumedName());
//	}


	
	public int getRecordCount(String table) throws SQLException, ClassNotFoundException {
		int numberOfRecords = -1;

		String command = "select count(*) from \"" + table + "\"";

		if ((runDBQuery(command)) && (this.getTableData().rows.get(0) != null)) {
			numberOfRecords = Integer.parseInt(getTableData().rows.get(0).cell.get(
					"count").getValue());
		}

		return numberOfRecords;
	}

	
	


	/**
	 * Add a row of data to a table
	 *
	 * @param tableName
	 *            the table where the row data should be added
	 * @param cols
	 *            an array of columns where the data is to reside
	 * @param cellData
	 *            an array of the data values. If a cell contains
	 *            Vars.NULL_VALUE then it is assumed to be a null value.
	 * @return true if successful
	 */
	public boolean addRowToTable(String tableName, String[] cols,
			String[] cellData, DatabaseQueries dq) throws ClassNotFoundException, SQLException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("addRowToTable(%s)", tableName));
		}

		//OrdsLangBundle lang = new OrdsLangBundle("lang", Locale.getDefault(),
		//		false);
		String primaryKey = getSingularPrimaryKeyColumn(tableName);

		if (cols == null) {
			log.error("No cols to add");
			return false;
		} else if (cellData == null) {
			log.error("No cellData to add");
			return false;
		}

		if (cellData.length == 0) {
			cellData = new String[cols.length];
		}

		/*
		 * If the user specifies '' then a blank is added. If the user specifies
		 * Vars.NULL_VALUE then a null is added. If the user specifies nothing
		 * (i.e.
		 * 
		 * Then the default value is used
		 */
		ArrayList colList = new ArrayList(Arrays.asList(cols));
		ArrayList dataList = new ArrayList(Arrays.asList(cellData));
		ArrayList empties = new ArrayList();
		String columns = "";
		String values = "";
		for (int index = 0; index < cols.length; index++) {
			if ((cellData[index] != null)
					&& (cellData[index].equals("[null value]"))) {
				log.warn("Null value in cell data");
				/*
				 * Null value
				 */
			}
			if (cellData[index] == null) {
				/*
				 * When data is passed in to ORDS, if nothing has been specified
				 * by the user then the values should be blank. However I
				 * sometimes see them as NULL. I don't currently know why.
				 */
				//TODO What is going on here???
				log.warn("Unexpected value of null here. This should not happen - let's make the value a blank for now.");
				cellData[index] = null;
//				continue;
			} else if (cellData[index].isEmpty()) {
				// Ignore this - let the database use the default value
				colList.remove(cols[index]);
				empties.add(cellData[index]);
				continue;
			}
			columns += "\"" + cols[index].trim() + "\",";
			values += "?,";
		}
		dataList.removeAll(empties);

		if (columns.isEmpty()) {
			/*
			 * This is a special case. Normally, if the user omits data in a
			 * column, the default value for that column will be used. However,
			 * That cannot happen for cases where the user omits all data in all
			 * columns, because then out insert command becomes invalid. So we
			 * assume this is a no-op and return true.
			 */
			log.info("No data entered - blanks needed");
			String command = String.format("insert into \"%s\" default values",
					tableName);
			return runDBQuery(command);
		}

		if (columns.endsWith(",")) {
			columns = columns.substring(0, columns.length() - 1);
		}
		if (values.endsWith(",")) {
			values = values.substring(0, values.length() - 1);
		}

		String command = String.format("insert into \"%s\" (%s) values (%s)",
				tableName, columns, values);

		Map<String, DataTypeMap> dataTypeMaps = getDataTypeMaps(tableName);
		for (DataTypeMap dataTypeMap : dataTypeMaps.values()) {
			int colIndex = colList.indexOf(dataTypeMap.colName);
			if (colIndex > -1) {
				dataTypeMap.index = colIndex;
				if ("[null value]".equals(
						dataList.get(colIndex))) {
					dataTypeMap.stringValue = null;
				} else {
					dataTypeMap.stringValue = (String) dataList.get(colIndex);
				}
			} else {
				dataTypeMaps.values().remove(dataTypeMap);
			}
		}

		// Insert blank - insert into table1 (col1,col2,col3,col4) values
		// ('a','b','c','');
		// Insert null - insert into table1 (col1,col2,col3) values
		// ('a','b','c');
		// or insert into table1 (col1,col2,col3,col4) values
		// ('a','b','c',null);

		boolean ret = false;

		if (dq == null) {
			ret = new DatabaseQueries(getCurrentDbServer(), getCurrentDbName())
			.createAndExecuteStatement(command, dataTypeMaps, tableName);
		}
		else {
			ret = dq.createAndExecuteStatement(command, dataTypeMaps, tableName);
		}

		if (!ret) {
			log.warn("Unable to update table successfully due to error.");
			log.warn("Trying again after resetting the sequence");
			if (!resetSequence(tableName, primaryKey)) {
				log.error("Unable to reset the sequence - will try the insert again, just in case");
			}
			if (dq == null) {
				ret = new DatabaseQueries(getCurrentDbServer(), getCurrentDbName()).createAndExecuteStatement(command, dataTypeMaps, tableName);
			}
			else {
				ret = dq.createAndExecuteStatement(command, dataTypeMaps, tableName);
			}
		}

		return ret;

	}

	private boolean resetSequence(String tableName, String index)
			throws ClassNotFoundException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("resetSequence(%s, %s)", tableName, index));
		}

		// Example:
		// SELECT setval(pg_get_serial_sequence('tblinvdetail','invdetailinvnum'),(SELECT GREATEST(MAX(invdetailinvnum)+1,nextval(pg_get_serial_sequence('tblinvdetail','invdetailinvnum')))-1 FROM tblinvdetail))
//		String sequenceResetCommand = String
//				.format("SELECT pg_catalog.setval(pg_get_serial_sequence('%s', '%s'), (SELECT MAX(%s) FROM \"%s\"));",
//						tableName, index, index, tableName);
		String sequenceResetCommand = String
				.format("SELECT setval(pg_get_serial_sequence('%s','%s'),(SELECT GREATEST(MAX(%s)+1,nextval(pg_get_serial_sequence('%s','%s')))-1 FROM %s));",
						tableName, index, index, tableName, index, tableName);
		boolean ret = false;
		try {
			ret = runDBQuery(sequenceResetCommand);
		} catch (SQLException ex) {
			log.error("Exception", ex);
		}

		return ret;
	}




	/**
	 * Takes the supplied file and parses it. The commands in the file will be
	 * Takes the supplied file and parses it. The commands in the file will be
	 * SQL commands, and this function will run each of them to create the
	 * database.
	 *
	 * @param schemaFile
	 * @return true if the file was successfully parsed and executed, else false
	 * @throws IOException
	 * @throws java.sql.SQLException
	 * @throws ClassNotFoundException
	 */
	public boolean loadFileContentsIntoDatabase(File schemaFile) throws IOException, SQLException,
			ClassNotFoundException {
		if (log.isDebugEnabled()) {
			log.debug("loadFileContentsIntoDatabase: "
					+ schemaFile.getAbsolutePath());
		}

		String fullSchemaCommand = FileUtils.readFileToString(schemaFile);
		if ((fullSchemaCommand == null)
				|| (fullSchemaCommand.trim().length() == 0)) {
			log.warn("No commands to run here. Assuming the table is empty!");
			return true;
		}
		/*
		 * Sometimes the commands might be of the form CREATE TABLE
		 * tblAllHospitals ( HospitalID SERIAL, Place character varying(255),
		 * primary key (HospitalID) ); CREATE TABLE tblApprenticeDoctor ( ...
		 * 
		 * and other times of the form
		 * 
		 * INSERT INTO tblPersonalIdentity VALUES(1,'Thame','PEC00 GMT
		 * 1598',1598); INSERT INTO tblPersonalIdentity VALUES(2,'Brit Mar 03
		 * 00:00:00 GMT 1685',1685);
		 * 
		 * So we can't necessarily split on \n or ;
		 * 
		 * Perhaps we can spilt on ;\n
		 */
		String[] commandlets = this
				.normalizeCommands(fullSchemaCommand);
		if (log.isDebugEnabled()) {
			log.debug(String.format("There are %d commands to run",
					commandlets.length));
		}

		for (String query : commandlets) {
			if (!runDBQuery(query)) {
				log.error("Problem running query " + query);
				dbCmdToBeRun = query;
				dbErrorMessage = getDbErrorMessage();
				return false;
			}
		}

		return true;
	}


	public Map<String, DataTypeMap> getDataTypeMaps(String tableName)
			throws ClassNotFoundException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("getDataTypeMaps(%s)", tableName));
		}

		Map<String, DataTypeMap> dataTypeMappingList = new ConcurrentHashMap<String, DataTypeMap>();
		try {
			runDBQuery("SELECT column_name, data_type FROM information_schema.columns where table_name=\'"
							+ tableName + "\'");

			for (DataRow rows : this.getTableData().rows) {
				DataTypeMap dtm = new DataTypeMap();
				dtm.colName = rows.cell.get("column_name").getValue();
				dtm.dt = QueryRunner.getDataType(rows.cell.get("data_type")
						.getValue());
				dataTypeMappingList.put(dtm.colName, dtm);
			}
		} catch (SQLException e) {
			log.error("Exception", e);
		}

		return dataTypeMappingList;
	}
	
	/**
	 * Get rows of data from the table. This function will get a subset of the
	 * table data.
	 *
	 * @param tableName
	 *            the table whose data is to be returned
	 * @param rowStart
	 *            the number of the first row to return. In terms of the user,
	 *            the row number starts at row 1, so for example they may
	 *            display rows 1 to 30, meaning the first 30 rows of data. In
	 *            terms of the database, however, the row number starts at 0, so
	 *            rows 1 to 30 (from a user perspective) correspond to rows 0 to
	 *            29 from a database perspective.
	 * @param numberOfRowsRequired
	 *            the number of rows to return. If zero, then all rows are
	 *            returned.
	 * @return table data or null if there has been an error or the table
	 *         doesn't exist
	 */
	public TableData getTableDataForTable(String tableName) throws ClassNotFoundException, SQLException {
		return getTableDataForTable(tableName, 1, 0);
	}
	public TableData getTableDataForTable(String tableName, int rowStart, int numberOfRowsRequired)
			throws ClassNotFoundException, SQLException {
		if (log.isDebugEnabled()) {
			log.debug(String.format("getTableDataForTable(%s, %d, %d)", tableName, rowStart, numberOfRowsRequired));
		}

		return getTableDataForTable(tableName, rowStart, numberOfRowsRequired, null, true);
	}	
	
	/**
	 * Delete one or more rows from a table
	 *
	 * @param tableName
	 *            the name of the table to operate on
	 * @param originalData
	 *            a map of the current data that is to be deleted
	 * @param tableData
	 *            the data in the table
	 * @return true if successful, else false
	 */
	public boolean deleteTableRow(String tableName, Row row) {
		if (tableName == null) {
			log.error("Null table name - can't be having that");
			return false;
		}
		if (log.isDebugEnabled()) {
			log.debug("deleteTableRow for table " + tableName);
			log.debug("These are the data columns to be deleted");
			log.debug("This is the data to be deleted");
			int i = 0;
			for (String name : row.columnNames) {
				log.debug("Deleting column: " + name + "data: "+row.values[i++]);
			}
		}

		String command = String.format("delete from \"%s\" where ", tableName);
		String whereString = "";
		int index = 0;
		for (String columnName : row.columnNames) {

			log.debug("Calling replace from delete row routine");
			whereString += String.format("\"%s\" = '%s' ", columnName,
						this.replaceSpecialCharacters(row.values[index++]));
			if (index < row.columnNames.length) {
				whereString += " and ";
			}
			else {
				whereString += ";";
			}
		}
		command += whereString;

		if (log.isDebugEnabled()) {
			log.debug("Command to run:" + command);
		}

		boolean ret = false;

		try {
			ret = runDBQuery(command);
		}
		catch (SQLException ex) {
			log.error("Exception", ex);
		}
		catch (ClassNotFoundException e) {
			log.error("Exception", e);
		}

		return ret;
	}

	
	/**
	 * Dump the current database directly to file
	 * @param file
	 * @param dbIP
	 * @return true if the database has been dumped successfully
	 * @throws IOException
	 */
	public boolean dumpDatabaseDirect(File file, String dbIP) throws IOException {
		boolean ret = false;

		// http://stackoverflow.com/questions/55709/streaming-large-files-in-a-java-servlet
		// http://stackoverflow.com/questions/685271/using-servletoutputstream-to-write-very-large-files-in-a-java-servlet-without-me
		log.debug("dumpDatabaseDirect");
		try {
			Process p;
			ProcessBuilder pb;

			// java.io.File file = new java.io.File(outputDir);
			/* We test if the path to our programs exists */
			if (file.exists()) {
				if (dbIP == null) {
					log.error("DBIP NULL");
					return false;
				}

				pb = new ProcessBuilder("pg_dump", "-f", file.toString(), 
						"-v", "-o", "-h", dbIP, "-U", getCurrentDbUser(), getCurrentDbName());
				if (log.isDebugEnabled()) {
					log.debug(String.format("About to run the command pg_dump -f %s -v -o -h %s -U %s %s",
							file.toString(), dbIP, getCurrentDbUser(), getCurrentDbName()));
				}
				pb.environment().put("PGPASSWORD", getCurrentDbPw());
				pb.redirectErrorStream(true);
				p = pb.start();
				try {
					InputStream is = p.getInputStream();
					InputStreamReader isr = new InputStreamReader(is);
					BufferedReader br = new BufferedReader(isr);
					String ll;
					log.debug("Logging output ...");
					while ((ll = br.readLine()) != null) {
						System.out.println(ll);
						if (log.isDebugEnabled()) {
							log.debug(ll);
						}
					}
					ret = true;
				}
				catch (IOException e) {
					log.error("ERROR ", e);
				}
			}

		}
		catch (IOException e) {
			log.error("Could not invoke browser, command=");
			log.error("Caught: " + e.getMessage());
		}

		log.debug("dumpDatabaseDirect:return");

		return ret;
	}
	
	
    /**
     * This will take a String of commands and return them as an array of individual commands.
     * It is assumed that each command starts on a new line but that commands may (or may not)
     * span many lines.
     * @param command the command(s) to process
     * @return an array of corresponding commands, one per array object. Null in, null out.
     */
    private  String[] normalizeCommands(String command) {
        log.debug("normalizeCommands");
        if (command == null) {
            log.error("Null input");
            return null;
        }
        List<String> result = new ArrayList<String>();
        
        String [] tempArray = command.split("\n");
        String individualCommand = "";
        for (String s : tempArray) {
            String s2 = s.trim();
            individualCommand += s2;
            if (s2.endsWith(";")) {
                // New command required
                result.add(individualCommand);
                individualCommand = "";
            }
        }
        
        
        return result.toArray(new String[result.size()]);
    }
    
    
    /**
	 * Postgres has some special characters that can frustrate database write
	 * attempts. We should Postgres-escape these here ...
	 *
	 * @param input
	 *            the input String that might contain Postgres-special
	 *            characters
	 * @return A String that should be Postgres-friendly
	 */
	private String replaceSpecialCharacters(String input) {
		if ((input != null) && (input.contains("'"))) {
			/*
			 * The character ' is a special one in Postgres - needs to be
			 * replaced by ''
			 */
			log.info("Input contains a single quote character");
			return input.replaceAll("'", "''");
		}
		else {
			// log.debug("Returning without change");
			return input;
		}
	}


}
