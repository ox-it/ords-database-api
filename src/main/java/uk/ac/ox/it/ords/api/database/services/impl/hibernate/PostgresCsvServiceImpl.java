package uk.ac.ox.it.ords.api.database.services.impl.hibernate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.SQLException;

import org.apache.commons.io.FileUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.conf.AuthenticationDetails;
import uk.ac.ox.it.ords.api.database.conf.CommonVars;
import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.queries.ORDSPostgresDB;
import uk.ac.ox.it.ords.api.database.queries.QueryRunner;
import uk.ac.ox.it.ords.api.database.services.CSVService;


public class PostgresCsvServiceImpl implements CSVService {

	private static Logger log = LoggerFactory.getLogger(PostgresCsvServiceImpl.class);

	private static final String EXPORT_FILE_PREFIX = "exportdata";
	private static final String EXPORT_FILE_SUFFIX = ".csv";

	
	
	@Override
	public File exportQuery(String server, String dbName, String query) throws Exception {
		
		//
		// Create the temporary file to export to
		//
		File file = File.createTempFile(EXPORT_FILE_PREFIX, EXPORT_FILE_SUFFIX);
		
		return exportQuery(server, dbName, query, file);
	}

	@Override
	public File exportQuery(String server, String dbName, String query, File file)
			throws Exception {
		
		//
		// Define the COPY operation to perform
		//
		String sql = String.format("COPY (%s) TO STDOUT WITH CSV HEADER ENCODING 'UTF8'", query);
		
		//
		// Perform export
		// 
		return exportData(server, dbName, sql, file);
	}

	@Override
	public File exportTable(String server, String dbName, String tableName) throws Exception {

		//
		// Create the temporary file to export to
		//
		File file = File.createTempFile(EXPORT_FILE_PREFIX, EXPORT_FILE_SUFFIX);

		//
		// Export the data
		//
		TableData tableData = new TableData();
		tableData.tableName = tableName;
		return exportTable(server, dbName, tableData, file);

	}
	
	@Override
	public File exportTable(String server, String dbName, TableData tableData) throws Exception {

		//
		// Create the temporary file to export to
		//
		File file = File.createTempFile(EXPORT_FILE_PREFIX, EXPORT_FILE_SUFFIX);

		//
		// Export the data
		//
		return exportTable(server, dbName, tableData, file);
	}
	
	

	@Override
	public File exportTable(String server, String dbName, TableData tableData, File file)
			throws Exception {
		if (tableData == null) {
			file.delete();
		}
		//
		// Define the copy operation
		//
		String sql = "COPY (SELECT * FROM "+tableData.tableName+ ") TO STDOUT WITH CSV HEADER ENCODING 'UTF8'";
		
		//
		// Perform export
		// 
		return exportData(server, dbName, sql, file);
	}
	
	/**
	 * Executes a COPY operation and reads the output stream to a file
	 * @param dbName
	 * @param sql
	 * @param file
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws Exception
	 */
	private File exportData(String server, String dbName, String sql, File file) throws IOException, ClassNotFoundException, SQLException, Exception{
		//
		// Get DBUtils instance for the specified database
		//
		//ORDSPostgresDBUtils dbUtils = new ORDSPostgresDBUtils(server, dbName, user, password);

		//
		// Get QueryRunner instance for the specified database
		//
		QueryRunner qr = new QueryRunner(null, dbName);

		//
		// Create a writer for outputting the CSV to the file, and get
		// the Postgres DB connection we'll use for reading the input from
		//
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(file), "UTF8"));
		PGConnection conn = (PGConnection)qr.getConnection();

		try {
			//
			// Obtain CopyManager from the Postgres Connection
			//
			CopyManager copyManager = conn.getCopyAPI();

			//
			// Copy out the data to the writer
			//
			copyManager.copyOut(sql, writer);
		} catch (Exception e) {
			file.deleteOnExit();
			throw new Exception("Error exporting table", e);
		} finally {
			//
			// Close writer
			//
			writer.close();

			//
			// Close DB connection
			//
			qr.getConnection().close();
		}
		return file;
	}
	



	@Override
	public TableData newTableDataFromFile(String server, String dbName, File file, boolean headerRow) throws Exception {
		return this.newTableDataFromFile(server, dbName, file.getName(), file, headerRow, true);
	}
	
	@Override
	public TableData newTableDataFromFile(String server, String dbName, String newTableName, File file, boolean headerRow) throws Exception {
		return this.newTableDataFromFile(server, dbName, newTableName, file, headerRow, true);
	}
	
	@Override
	public TableData newTableDataFromFile(String server, String dbName, String newTableName, File file,
			boolean headerRow, boolean addPrimaryKeyColumn) throws Exception {

		
		//
		// If no table name is provided, just use the file name
		//
		if (newTableName == null) newTableName = file.getName();
		
		//
		// Get the normalised name for the table
		//
		String tableName = getUniqueTableName(server, dbName, newTableName);

		log.debug("Using a table name of " + tableName);
		
		//
		// Create new table structure using the first row of the CSV file as headers (or placeholders if no header row)
		//
		String[] columnNames =  getColumnNames(server, dbName, tableName, file, headerRow);
		createTableStructure(server, dbName, tableName, columnNames);
		
		//
		// Load the data using COPY
		//
		try {
			appendDataFromFile(server, dbName, tableName, file, headerRow);
		} catch (Exception e) {
			//
			// If we can't load the data, drop the table
			//
			dropTable(server, dbName, tableName);
			log.error(e.getMessage());
			throw e;
		}
		
		//
		// Add a primary key column
		//
		if (addPrimaryKeyColumn){
		  if (!addPrimaryKeyToCSVTable(server, dbName, tableName)) {
			log.error("Unable to add primary key to table");
			throw new Exception(
					"Unable to add primary key to table");
		  }
		}
		
		TableData tableData = new TableData();
		tableData.tableName = tableName;
		return tableData;
	}

	@Override
	public void appendDataFromFile(String server, String dbName, String tableName, File file) throws Exception{
		appendDataFromFile(server, dbName, tableName, file, true);
	}
	
	@Override
	public void appendDataFromFile(String server, String dbName, String tableName, File file, boolean headerRow) throws Exception{

		//
		// Get DBUtils instance for the specified database
		//
		//ORDSPostgresDBUtils dbUtils = new ORDSPostgresDBUtils(server, dbName, user, password);

		//
		// Get QueryRunner instance for the specified database
		//
		QueryRunner qr = new QueryRunner(null, dbName);

		//
		// Define the copy operation
		//
		String sql = "COPY "+tableName+" FROM STDIN WITH CSV ENCODING 'UTF8'";
		if (headerRow) sql += " HEADER";

		//
		// Create a reader for getting the data from the file and get
		// the Postgres DB connection we'll use for writing the output
		//
		BufferedReader reader = new BufferedReader( new InputStreamReader( new FileInputStream(file), "UTF8"));

		PGConnection conn = (PGConnection)qr.getConnection();

		try {
			//
			// Obtain CopyManager from the Postgres Connection
			//
			CopyManager copyManager = conn.getCopyAPI();	
			//
			// Copy in the data
			//
			copyManager.copyIn(sql, reader);
			
		} catch (Exception e) {
			throw new Exception("Error importing table", e);
		} finally {
			//
			// Close writer
			//
			reader.close();

			//
			// Close DB connection
			//
			qr.getConnection().close();
		}
	}

	/**
	 * Create a new table based on the file contents
	 * @param dbServer
	 * @param dbName
	 * @param tableName
	 * @param file
	 * @param headerRow
	 * @throws Exception
	 */
	private String[] getColumnNames(String dbServer, String dbName, String tableName, File file, boolean headerRow) throws Exception{
		String header = null;
		String[] columnNames = null;
		
		try {	
			//
			// Get the first line and split by commas using escape rules
			//
			header = FileUtils.readLines(file, "UTF8").get(0);
			columnNames = header.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		} catch (IOException e) {
			throw new Exception("Could not read header row", e);
		}
		if (!headerRow){
			columnNames = new String[columnNames.length];
		}

		return columnNames;
	}

	/**
	 * Create a Postgres table from CSV data.
	 *
	 * @param tableName
	 * @param ocsv
	 * @param opd
	 * @param firstRowColNames
	 * @param conn
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws CreateTableException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private String[] createTableStructure(String dbServer, String dbName,
			String tableName, String[] columnNames)
					throws Exception {

		if (createDBTableWithDummyColumn(dbServer, dbName, tableName)) {

			for (int count = 0; count < columnNames.length; count++) {
				if (columnNames[count] == null || columnNames[count].trim().isEmpty()) {
					columnNames[count] = "Column_" + count;
				}
				if (!addColumnToTable(dbServer, dbName, tableName, columnNames[count])) {
					log.error("Unable to create column in table");
					throw new Exception(
							"Unable to create column in table");
				}
			}
			if (!removeDummyColumnFromTable(dbServer, dbName, tableName)) {
				log.error("Unable to remove dummy column from table");
				throw new Exception(
						"Unable to remove dummy column from table");
			}
		} else {
			throw new Exception("Unable to create a table within the database");
		}
		return columnNames;
	}

	private static final String DUMMY_COLUMN_NAME = "willRemoveThisSoon_dummy";

	private boolean dropTable(String dbServer, String dbName, String tableName) throws ClassNotFoundException, SQLException{
		String command = String.format("drop table \"%s\";",tableName);
		return new QueryRunner(dbServer, dbName).runDBQuery(command);
	}
	
	//
	// Create a blank table with a single dummy column
	//
	private boolean createDBTableWithDummyColumn(String dbServer, String dbName, String tableName)
            throws ClassNotFoundException, SQLException {
		String command = String.format("create table \"%s\" (\"%s\" integer);",
				tableName, DUMMY_COLUMN_NAME);

        QueryRunner qr = new QueryRunner(dbServer, dbName);
        return qr.runDBQuery(command);
		//return qr.runDBQuery(String.format("alter table  \"%s\" owner to \"%s\"", tableName));
	}
	
	//
	// remove the dummy column
	//
	private boolean removeDummyColumnFromTable(String dbServer, String dbName,
			String tableName) throws ClassNotFoundException, SQLException {
		log.debug("removeDummyColumnFromTable");

		String command = String.format("alter table \"%s\" drop column \"%s\"",
				tableName, DUMMY_COLUMN_NAME);

		return new QueryRunner(dbServer, dbName).runDBQuery(command);
	}

	/**
	 * Add column to database
	 * @param dbServer
	 * @param dbName
	 * @param tableName
	 * @param columnName
	 * @param type
	 * @param defaultValue
	 * @param nullAllowed
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private boolean addColumnToTable(String dbServer, String dbName, String tableName, String columnName)
					throws ClassNotFoundException, SQLException {
		log.debug("addColumnToTable");

		//
		// Avoid double-quotes - if a column name is in quotes, then PG will
		// add more quotes around it causing the query to fail
		//
		if (columnName.startsWith("\"") && columnName.endsWith("\"")){
			columnName = columnName.substring(1, columnName.length()-1);
		}
		
		String sql = String.format("alter table \"%s\" add column \"%s\" text", tableName, columnName.trim());
		if (!new QueryRunner(dbServer, dbName).runDBQuery(sql, 0, 0)) {
			return false;
		}
		
		sql = String.format("alter table \"%s\" alter column \"%s\" drop not null",tableName,columnName.trim());
		if (!new QueryRunner(dbServer, dbName).runDBQuery(sql, 0, 0)) {
			return false;
		}


		return true;
	}
	
	/**
	 * Add a primary key to a CSV table
	 *
	 * @param tableName
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private boolean addPrimaryKeyToCSVTable(String dbServer, String dbName, String tableName)
			throws ClassNotFoundException, SQLException {
		log.debug("addPrimaryKeyToCSVTable");

		String[] commands = {
				String.format("alter table \"%s\" add COLUMN %s serial",
						tableName, tableName + "_index"),
				String.format("alter table \"%s\" ADD PRIMARY KEY (%s)",
						tableName, tableName + "_index") };

		for (String s : commands) {
			if (!new QueryRunner(dbServer, dbName).runDBQuery(s)) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * Calculate the unique name for a table. This is useful in the case where a
	 * second CSV file is uploaded to a database. Since the CSV file name
	 * becomes the db table name, this might cause problems in the event where
	 * there is already a table with that name. This will understand that.
	 *
	 * @param tableNameSeed
	 *            the name of the table to check
	 * @return a unique (in the database) table name. This might be the same
	 *         name as the input table if it is already unique.
	 * @throws java.sql.SQLException
	 * @throws java.lang.ClassNotFoundException
	 */
	private String getUniqueTableName(String dbServer, String dbName, String tableNameSeed) throws SQLException,
			ClassNotFoundException {
		
		log.debug("getUniqueTableName:" + tableNameSeed);
		
		/*
		 * First we need to ensure the tablename dows not have any special
		 * characters that will confuse or upset Postgres.
		 */
		tableNameSeed = this
				.removeBadCharsFromName(tableNameSeed.toLowerCase());
		
		int counter = 2;
		boolean foundThis;
		String newTableName = tableNameSeed;
		
		ORDSPostgresDB dbUtils = new ORDSPostgresDB(dbServer, dbName);

		if (dbUtils.getTablesForDatabase()) {
			TableData tableData = dbUtils.getTableData();
	
			while (true) {
				foundThis = false;
				for (DataRow dr : tableData.rows) { // Look through each
																// table name in the
																// database
					for (String s : dr.cell.keySet()) {
						if (dr.cell.get(s).getValue()
								.equalsIgnoreCase(newTableName)) {// We already have
																	// this table
																	// present
							foundThis = true;
							break;
						}
					}
					if (foundThis) {
						break;
					}
				}
	
				if (foundThis) {
					newTableName = String.format("%s_%d", tableNameSeed, counter++);
				} else {
					break;
				}
			}
		}

		log.debug("getUniqueTableName:Returning " + newTableName);
		
		return newTableName;
	}
	
	
    private  String removeBadCharsFromName(String name) {
        String ret, temp;
        
        // First, remove characters after the first "." (so that, for example, fred.csv becomes fred
        if (name.contains(".")) {
            temp = name.substring(0, name.indexOf("."));
        }
        else {
            temp = name;
        }
        
        // Now remove "+" and spaces and dashes and % signs
        ret = temp.replace("@", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("'", CommonVars.BAD_CHARACTER_REPLACEMENT)
        		.replace("~", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("#", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("+", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("{", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("}", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("{", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("}", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("(", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace(")", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("*", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("&", CommonVars.BAD_CHARACTER_REPLACEMENT).replace(" ", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("-", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("%", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("!", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("\"", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("^", CommonVars.BAD_CHARACTER_REPLACEMENT).replace(";", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace(":", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("<", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace(">", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("$", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("��", CommonVars.BAD_CHARACTER_REPLACEMENT).replace("[", CommonVars.BAD_CHARACTER_REPLACEMENT)
                .replace("]", CommonVars.BAD_CHARACTER_REPLACEMENT);
        
        // Make sure the name does not start with a numberic
        try {
            Integer.parseInt(name.substring(0, 1));
            ret = "_" + ret;
        }
        catch (NumberFormatException e) {
            // fine
        }
        
        return ret;
    }




}
