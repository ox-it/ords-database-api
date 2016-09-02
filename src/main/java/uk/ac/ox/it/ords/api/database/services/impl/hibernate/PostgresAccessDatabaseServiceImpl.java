package uk.ac.ox.it.ords.api.database.services.impl.hibernate;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.queries.QueryRunner;
import uk.ac.ox.it.ords.api.database.services.AccessImportService;
import uk.ac.ox.it.ords.api.database.services.TableImportResult;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Relationship;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public class PostgresAccessDatabaseServiceImpl implements AccessImportService {

	private static final String DUMMY_COLUMN_NAME = "willRemoveThisSoon_dummy";
	private Logger log = LoggerFactory
			.getLogger(PostgresAccessDatabaseServiceImpl.class);

	public PostgresAccessDatabaseServiceImpl() {
		sequences = new ArrayList<Pair<String, String>>();
	}
	
	public ArrayList<Pair<String, String>> sequences;

	@Override
	public boolean preflightImport(File database) {
		if (database == null || !database.exists()) {
			log.debug("No database was supplied for preflight");
			return false;
		}
		try {
			Map<String, TableImportResult> results = parseSchema(database,
					false, null, null);
			for (TableImportResult result : results.values()) {
				if (result.getTableCreateResult() == TableImportResult.FAILED)
					return false;
			}
			return true;
		} catch (IOException e) {
			log.debug("Error preflighting database file");
			return false;
		}
	}

	@Override
	public Map<String, TableImportResult> createSchema(String databaseServer,
			String databaseName, File database) {
		if (database == null || !database.exists()) {
			log.debug("No database was supplied for schema creation");
			return null;
		}
		try {
			return parseSchema(database, true, databaseServer, databaseName);
		} catch (IOException e) {
			log.debug("Error importing database file");
			return null;
		}
	}

	@Override
	public Map<String, TableImportResult> importData(String databaseServer,
			String databaseName, File file) {

		//
		// intialise the results map
		//
		Map<String, TableImportResult> results = new HashMap<String, TableImportResult>();

		//
		// Check we have a file
		//
		if (file == null || !file.exists()) {
			log.debug("No database was supplied for data import");
			return results;
		}

		//
		// Open the DB
		//
		try {
			Database database = DatabaseBuilder.open(file);
			List<String> tablesToOmit = new ArrayList<String>();

			//
			// First, do a dry run
			//
			//AuthenticationDetails ad = new AuthenticationDetails();
			results = importData(databaseServer, databaseName, database,tablesToOmit, true);

			//
			// Identify tables with problems; we'll omit these
			//
			for (String key : results.keySet()) {
				if (results.get(key).getDataImportResult() == TableImportResult.FAILED) {
					tablesToOmit.add(key);
				}
			}

			log.debug("import preflight complete; " + tablesToOmit.size()
					+ " tables to omit in full run");

			//
			// Now re-run and commit. This should ensure all data is imported
			// that passed preflight.
			//
			Map<String, TableImportResult> finalResults = importData(
					databaseServer, databaseName, database, tablesToOmit, false);

			//
			// Merge results
			//
			for (String key : results.keySet()) {
				if (finalResults.containsKey(key)) {
					results.get(key).merge(finalResults.get(key));
				}
			}

			//
			// Now lets verify the import. While we know how many rows we
			// INSERTed in the batches,
			// once we committed them Postgres will discard rows that break any
			// table constraints.
			//
			for (String tableName : results.keySet()) {
				QueryRunner qr = new QueryRunner(databaseServer, databaseName);
				qr.runDBQuery(String.format("SELECT COUNT(*) FROM \"%s\"",
						getNormalisedTableName(tableName)));
				TableData tableData = qr.getTableData();
				int rowsVerified = Integer.parseInt(tableData.rows.iterator()
						.next().cell.get("count").getValue());
				results.get(tableName).setRowsVerified(rowsVerified);
			}
			
			//
			// Finally, we have to update the sequences
			//
			for (Pair<String, String> sequence : sequences){
				System.out.println(sequence.getLeft() + ":" + sequence.getRight());
				QueryRunner qr = new QueryRunner(databaseServer, databaseName);
				String key = getNormalisedColumnName(sequence.getLeft());
				String table = getNormalisedTableName(sequence.getRight());
				String sql = "SELECT pg_catalog.setval(pg_get_serial_sequence('%s', '%s'), MAX(\"%s\")) FROM \"%s\";";
				qr.runDBQuery(String.format(sql, table, key, key, table));
			}

		} catch (IOException | SQLException | ClassNotFoundException
				| NumberFormatException e) {
			log.debug("Error importing database file", e);
		}
		return results;
	}

	private Map<String, TableImportResult> importData(String databaseServer,
			String databaseName, Database database, List<String> tablesToOmit, boolean preflight)
			throws SQLException {
		//
		// intialise the results map
		//
		Map<String, TableImportResult> results = new HashMap<String, TableImportResult>();

		Connection conn = null;
		PreparedStatement defer = null;

		try {

			//
			// Start a transaction
			//
			QueryRunner qr = new QueryRunner(databaseServer, databaseName);
			conn = qr.getConnection();
			conn.setAutoCommit(false);

			//
			// Import each table in turn
			//
			for (String tableName : database.getTableNames()) {
				if (!excludeTable(tableName)
						&& !tablesToOmit
								.contains(getNormalisedTableName(tableName))) {

					//
					// Defer foreign key checks. In this way Postgres won't
					// attempt
					// to do any referential integrity checks until we commit
					// the whole
					// import.
					//
					defer = conn
							.prepareStatement("SET CONSTRAINTS ALL DEFERRED;");
					defer.executeUpdate();

					//
					// Run the import batch. If it fails for any reason, we
					// record a
					// "false" (import failed) for that row in the result map
					// and keep going
					// - however to do this we have to close the current
					// transaction, rollback
					// changes so far and try again.
					//
					TableImportResult result = importDataForTable(conn,
							database.getTable(tableName));
					results.put(getNormalisedTableName(tableName), result);
					if (result.getDataImportResult() == TableImportResult.FAILED) {
						log.debug("Couldn't import data for table " + tableName);
						conn.rollback();
						conn = qr.getConnection();
						conn.setAutoCommit(false);
					}
				}
			}

			//
			// Commit changes if we aren't doing a preflight
			//
			if (preflight) {
				conn.rollback();
			} else {
				conn.commit();
			}
		} catch (ClassNotFoundException | SQLException | IOException e) {
			if (conn != null) {
				try {
					log.debug("Transaction is being rolled back");
					conn.rollback();
				} catch (SQLException excep) {
					log.debug("Error rolling back transaction");
				}
			}
			log.error("Problem importing data", e);
		} finally {
			if (defer != null)
				defer.close();
			if (conn != null)
				conn.close();
		}

		return results;

	}

	private TableImportResult importDataForTable(Connection conn, Table table)
			throws IOException, ClassNotFoundException, SQLException {
		String tableName = getNormalisedTableName(table.getName());
		TableImportResult result = new TableImportResult();

		//
		// How many columns are we dealing with here? We need to create
		// placeholders for the prepared statement,
		// so we add a "?, " for each.
		//
		int columns = table.getColumnCount();
		String params = "";
		for (int i = 0; i < columns; i++) {
			params += "?, ";
		}
		//
		// Chop off the trailing comma
		//
		params = params.substring(0, params.lastIndexOf(","));

		//
		// Add our list of placeholders to the INSERT statement
		//
		String sql = String.format("INSERT INTO \"%s\" VALUES (%s)", tableName,
				params);
		PreparedStatement pstmt = conn.prepareStatement(sql);

		//
		// Set up prepared statement. We'll reuse this for each row
		//
		pstmt = conn.prepareStatement(sql);

		//
		// Set the number of rows we have from the file
		//
		result.setRowsReceived(table.getRowCount());

		//
		// Import each row
		//
		int rowIndex = 0;
		for (Row row : table.newCursor().toCursor()) {
			rowIndex++;
			addRowToPreparedStatement(table, row, pstmt);
			//
			// Execute for every 1000 rows as we go
			//
			if ((rowIndex + 1) % 1000 == 0) {
				try {
					pstmt.executeBatch();
				} catch (java.sql.BatchUpdateException e) {
					e.getNextException().printStackTrace();
					log.error("Error importing data", e.getNextException());
					result.setDataImportResult(TableImportResult.FAILED);
					result.setRowsImported(0);
					result.addException(e.getNextException());
				}
			}
			result.setRowsImported(rowIndex);
		}

		//
		// Run all remaining INSERTs in the batch
		//
		try {
			pstmt.executeBatch();
			result.setDataImportResult(TableImportResult.SUCCESSFUL);
		} catch (java.sql.BatchUpdateException e) {
			log.error("Error importing data", e.getNextException());
			result.setDataImportResult(TableImportResult.FAILED);
			result.setRowsImported(0);
			result.addException(e.getNextException());
		} finally {
			pstmt.close();
			pstmt = null;
		}

		return result;
	}

	/**
	 * Adds a single row to a prepared statement. Consider merging this code
	 * with DatabaseQueries.prepareAndExecuteStatement and
	 * QueryRunner.addParametersToPreparedStatement
	 * 
	 * @param table
	 * @param pstmt
	 * @param failedRows
	 * @throws SQLException
	 * @throws IOException
	 */
	private void addRowToPreparedStatement(Table table, Row row,
			PreparedStatement pstmt) throws SQLException, IOException {
		int index = 0;
		//
		// Set a parameter for each cell. For some types we have to
		// do an explicit NULL check here.
		//
		for (Column column : table.getColumns()) {
			index++;
			DataType type = column.getType();
			String columnName = column.getName();
			switch (type) {
				case BINARY :
					if (row.getInt(columnName) == null) {
						pstmt.setNull(index, Types.INTEGER);
					} else {
						pstmt.setInt(index, row.getInt(columnName));
					}
					break;
				case BOOLEAN :
					pstmt.setBoolean(index, row.getBoolean(columnName));
					break;
				case BYTE :
					pstmt.setByte(index, row.getByte(columnName));
					break;
				case DOUBLE :
					if (row.getDouble(columnName) == null) {
						pstmt.setNull(index, Types.DOUBLE);
					} else {
						pstmt.setDouble(index, row.getDouble(columnName));
					}
					break;
				case FLOAT :
					pstmt.setFloat(index, row.getFloat(columnName));
					break;
				case INT :

					if (row.getShort(columnName) == null) {
						pstmt.setNull(index, Types.INTEGER);
					} else {
						pstmt.setShort(index, row.getShort(columnName));
					}
					break;
				case LONG :
					if (row.getInt(columnName) == null) {
						pstmt.setNull(index, Types.BIGINT);
					} else {
						pstmt.setLong(index, row.getInt(columnName));
					}
					break;
				case MONEY :
					pstmt.setBigDecimal(index, row.getBigDecimal(columnName));
					break;
				case NUMERIC :
					pstmt.setFloat(index, row.getFloat(columnName));
					break;
				case OLE :
					pstmt.setBytes(index, row.getBytes(columnName));
					break;
				case SHORT_DATE_TIME :
					if (row.getDate(columnName) == null) {
						pstmt.setNull(index, Types.DATE);
					} else {
						pstmt.setDate(index,
								new java.sql.Date(row.getDate(columnName)
										.getTime()));
					}
					break;
				default :
					pstmt.setString(index, row.getString(columnName));
					break;
			}

		}

		//
		// Add the statement to the batch
		//
		pstmt.addBatch();
	}

	/**
	 * Interpret the schema and create postgres structures
	 * 
	 * @param databaseFile
	 * @param commit
	 * @param serverName
	 * @param databaseName
	 * @return a map of TableImportResult instances with table creation results
	 * @throws IOException
	 */
	private Map<String, TableImportResult> parseSchema(File databaseFile,
			boolean commit, String serverName, String databaseName)
			throws IOException {
		Database database = DatabaseBuilder.open(databaseFile);

		Map<String, TableImportResult> results = new HashMap<String, TableImportResult>();

		//
		// Tables
		//
		Set<String> tablesName = database.getTableNames();
		Iterator<String> tables = tablesName.iterator();
		while (tables.hasNext()) {
			Table table = database.getTable(tables.next());
			TableImportResult result = new TableImportResult();
			if (!excludeTable(table.getName())) {
				if (!createTableStructure(table, commit, serverName,
						databaseName)) {
					log.error("Unable to create table structure");
					result.setTableCreateResult(TableImportResult.FAILED);
				} else {
					result.setTableCreateResult(TableImportResult.SUCCESSFUL);
				}
			}
			results.put(getNormalisedTableName(table.getName()), result);
		}

		//
		// Constraints
		//
		Map<String, TableImportResult> constraintResults = createForeignKeys(
				serverName, databaseName, database, commit);

		//
		// Merge results
		//
		for (String key : results.keySet()) {
			results.get(key).merge(constraintResults.get(key));
		}

		return results;
	}

	/**
	 * Adds a column to a table.
	 * 
	 * @param server
	 * @param database
	 * @param table
	 * @param columnName
	 * @param dataType
	 * @return true if the column is successfully added
	 */
	private boolean addColumnToTable(String server, String database, String table, String columnName,
			String dataType) {

		columnName = getNormalisedColumnName(columnName);

		String sql = String.format("alter table \"%s\" add column \"%s\" %s",
				table, columnName.trim(), dataType);
		try {
			return (new QueryRunner(server, database)
					.runDBQuery(sql, 0, 0, false));
		} catch (ClassNotFoundException | SQLException e) {
			log.debug("Error creating table column", e);
			return false;
		}
	}

	/**
	 * If we want to change table names on import we can put the rules here For
	 * now I'm just changing to lowercase but otherwise leaving intact
	 * 
	 * @param tableName
	 * @return
	 */
	private String getNormalisedTableName(String tableName) {
		return tableName.toLowerCase();
	}

	private String getNormalisedColumnName(String columnName) {
		//
		// Avoid double-quotes - if a column name is in quotes, then PG will
		// add more quotes around it causing the query to fail
		//
		if (columnName.startsWith("\"") && columnName.endsWith("\"")) {
			columnName = columnName.substring(1, columnName.length() - 1);
		}
		return columnName;
	}

	/**
	 * Creates a primary key constraint
	 * 
	 * @param server
	 * @param database
	 * @param table
	 * @param columnNames
	 * @return true if successful
	 */
	private boolean addPrimaryKeysToTable(String server, String database, String table, List<String> columnNames) {
		String columns = "";
		for (String column : columnNames) {
			columns += ("\"" + getNormalisedColumnName(column) + "\", ");
		}
		columns = columns.substring(0, columns.lastIndexOf(","));

		String command = String.format(
				"alter table \"%s\" ADD PRIMARY KEY (%s)", table, columns);
		try {
			return (new QueryRunner(server, database)
					.runDBQuery(command, 0, 0, false));
		} catch (ClassNotFoundException | SQLException e) {
			log.debug("Error creating primary key", e);
			return false;
		}
	}

	/**
	 * Create a new table
	 * 
	 * @param table
	 * @param commit
	 * @param serverName
	 * @param databaseName
	 * @return true if successful
	 */
	private boolean createTableStructure(Table table, boolean commit,
			String serverName, String databaseName) {
		log.debug("createTableStructure");

		if (table == null) {
			log.error("Null table provided. Unable to work with this.");
			return false;
		}

		String tableName = getNormalisedTableName(table.getName());

		//
		// Create the table with a dummy column
		//
		String command = String.format("create table \"%s\" (\"%s\" integer);",
				tableName, DUMMY_COLUMN_NAME);

		//
		// Actually create the table
		//
		if (commit) {
			try {
				/*
				 * We need to use the root creds to create tables. This is
				 * because only using those creds do the correct ODBC
				 * permissions get used as set up in the schema. Surely that
				 * shouldn't be?
				 */
				QueryRunner qr = new QueryRunner(serverName, databaseName);
				if (qr.runDBQuery(command) == false) {
					return false;
				}
			} catch (ClassNotFoundException | SQLException e) {
				log.debug("Error trying to create table", e);
				return false;
			}
		}

		//
		// Create each column
		//
		for (Column column : table.getColumns()) {
			if (!(column.getName().startsWith("s_") || column.getName()
					.startsWith("Gen_"))) {
				if (commit) {
					if (column.isAutoNumber()) {
						addColumnToTable(serverName, databaseName, tableName,
								column.getName(), "SERIAL");
						
						//
						// Autonumber columns need some extra work, so we flag them
						// for later
						//
						Pair<String, String> sequence = Pair.of(column.getName(), tableName);
						sequences.add(sequence);
					} else {
						addColumnToTable(serverName, databaseName,tableName,
								column.getName(),
								getPostgresTypeForAccessDataType(column
										.getType()));
					}
				}
			} else {
				log.info(String.format(
						"Column <%s> is not suitable for processing",
						column.getName()));
			}
		}

		//
		// Add primary keys
		//
		List<String> primaryKeyList = getPrimaryKeys(table);
		if (commit && !primaryKeyList.isEmpty()) {
			addPrimaryKeysToTable(serverName, databaseName, tableName,
					primaryKeyList);
		}

		//
		// Remove dummy column
		//
		command = String.format("alter table \"%s\" drop column \"%s\"",
				tableName, DUMMY_COLUMN_NAME);
		if (commit) {
			try {
				return new QueryRunner(serverName, databaseName).runDBQuery(command);
			} catch (ClassNotFoundException | SQLException e) {
				log.debug("Error removing dummy column", e);
				return false;
			}
		}

		return true;
	}

	/**
	 * 
	 * Creates all foreign keys between all tables
	 * 
	 * @param serverName
	 * @param databaseName
	 * @param database
	 * @param commit
	 *            if true, will commit changes; otherwise will simply record
	 *            whether obtaining the data from the file was successful
	 * @return a map of TableImportResult instances with the foreign key stats
	 *         completed
	 * @throws IOException
	 */
	private Map<String, TableImportResult> createForeignKeys(String serverName,
			String databaseName, Database database, boolean commit) throws IOException {

		Map<String, TableImportResult> results = new HashMap<String, TableImportResult>();

		List<Relationship> relationships = database.getRelationships();

		Iterator<String> fromTables = database.getTableNames().iterator();
		Iterator<String> toTables = database.getTableNames().iterator();

		//
		// Go through all combinations of tables and set the outcome to
		// SUCCESSFUL - if
		// we do come across a problem we reset it to FAILED
		//
		try {
			while (fromTables.hasNext()) {
				String fromTable = fromTables.next();
				results.put(getNormalisedTableName(fromTable),
						new TableImportResult());
				results.get(getNormalisedTableName(fromTable))
						.setTableConstraintResult(TableImportResult.SUCCESSFUL);
				while (toTables.hasNext()) {
					String toTable = toTables.next();
					results.put(getNormalisedTableName(toTable),
							new TableImportResult());
					results.get(getNormalisedTableName(toTable))
							.setTableConstraintResult(
									TableImportResult.SUCCESSFUL);
				}
			}
		} catch (Exception e) {
			log.error("Error reading relatioships", e);
			for (TableImportResult result : results.values()) {
				result.setTableConstraintResult(TableImportResult.FAILED);
			}
			return results;
		}

		//
		// Actually create the relations in PostgreSQL
		//
		if (commit) {
			for (Relationship relationship : relationships) {
				try {
					if (!createForeignKeyRelation(serverName, databaseName,relationship)) {
						results.get(
								getNormalisedTableName(relationship
										.getFromTable().getName()))
								.setTableConstraintResult(
										TableImportResult.FAILED);
					} else {
						int constraints = results.get(
								getNormalisedTableName(relationship
										.getFromTable().getName()))
								.getConstraintsAdded() + 1;
						results.get(
								getNormalisedTableName(relationship
										.getFromTable().getName()))
								.setConstraintsAdded(constraints);
					}
				} catch (ClassNotFoundException e) {
					results.get(
							getNormalisedTableName(relationship.getFromTable()
									.getName())).setTableConstraintResult(
							TableImportResult.FAILED);
					results.get(
							getNormalisedTableName(relationship.getFromTable()
									.getName())).addException(e);
				} catch (SQLException e) {
					results.get(
							getNormalisedTableName(relationship.getFromTable()
									.getName())).setTableConstraintResult(
							TableImportResult.FAILED);
					results.get(
							getNormalisedTableName(relationship.getFromTable()
									.getName())).addException(e);
				}
			}
		}

		return results;
	}

	/**
	 * Creates a foreign key constraint.
	 * 
	 * @param server
	 * @param database
	 * @param relationship
	 * @return true if the relationship was created, false if there was any
	 *         problem with the query
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	private boolean createForeignKeyRelation(String server, String database,
			Relationship relationship)
			throws ClassNotFoundException, SQLException {
		String fromColumn = relationship.getFromColumns().get(0).getName();
		String toColumn = relationship.getToColumns().get(0).getName();
		String fromTable = getNormalisedTableName(relationship.getFromTable()
				.getName());
		String toTable = getNormalisedTableName(relationship.getToTable()
				.getName());
		String command = ("ALTER TABLE \"" + toTable + "\" ADD CONSTRAINT "
				+ relationship.getName() + " FOREIGN KEY (\"" + toColumn
				+ "\") REFERENCES \"" + fromTable + "\" (\"" + fromColumn + "\") ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED; \n");
		return new QueryRunner(server, database)
				.runDBQuery(command);
	}

	/**
	 * Maps Access dataTypes to PostgreSQL datatypes
	 * 
	 * @param type
	 * @return
	 */
	private String getPostgresTypeForAccessDataType(DataType type) {
		if (type.equals(DataType.BOOLEAN))
			return "bool";
		if (type.equals(DataType.BINARY))
			return "int2";
		if (type.equals(DataType.BYTE))
			return "int2";
		if (type.equals(DataType.COMPLEX_TYPE))
			return "bit";
		if (type.equals(DataType.DOUBLE))
			return "double precision";
		if (type.equals(DataType.FLOAT))
			return "float4";
		if (type.equals(DataType.GUID))
			return "character varying(255)";
		if (type.equals(DataType.INT))
			return "int4";
		if (type.equals(DataType.LONG))
			return "int8";
		if (type.equals(DataType.MEMO))
			return "text";
		if (type.equals(DataType.MONEY))
			return "numeric";
		if (type.equals(DataType.NUMERIC))
			return "decimal(20,4)";
		if (type.equals(DataType.OLE))
			return "bytea";
		//
		// Note that we can't tell if its really a DATE, TIME or TIMESTAMP. So
		// users will
		// have to use the schema editor to change it as appropriate.
		//
		if (type.equals(DataType.SHORT_DATE_TIME))
			return "timestamp";
		if (type.equals(DataType.TEXT))
			return "character varying(255)";
		return "text";
	}

	/**
	 * Get all primary keys for the table
	 * 
	 * @param table
	 * @return a list of primary keys
	 */
	private List<String> getPrimaryKeys(Table table) {
		log.debug("getPrimaryKeys");
		List<String> primaryKeyList = new ArrayList<String>();

		for (Index index : table.getIndexes()) {
			for (Index.Column column : index.getColumns()) {
				if (index.isPrimaryKey()) {
					primaryKeyList.add(column.getName());
				}
			}
		}
		return primaryKeyList;
	}

	/**
	 * Determine if the table should be excluded from import
	 * 
	 * @param tableName
	 * @return true if the table should be excluded
	 */
	private boolean excludeTable(String tableName) {
		if (tableName.trim().startsWith("~TMP"))
			return true;
		if (tableName.contains("_Conflict"))
			return true;
		if (tableName.equals("Switchboard Items"))
			return true;
		return false;
	}

}
