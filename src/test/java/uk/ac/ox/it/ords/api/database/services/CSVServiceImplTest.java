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
package uk.ac.ox.it.ords.api.database.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.conf.CommonVars;
import uk.ac.ox.it.ords.api.database.data.OrdsTableColumn;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.queries.ORDSPostgresDB;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;
import uk.ac.ox.it.ords.security.services.ServerConfigurationService;

public class CSVServiceImplTest {
	
	Configuration properties = MetaConfiguration.getConfiguration();


	@BeforeClass
	public static void setUp(){
		
	}
	
	@After
	public void tearDown() throws Exception{		
		//
		// Delete the table afterwards
		//
		new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				).runDBQuery("DROP TABLE IF EXISTS assetsimportcompletesample");
	}

	@Test
	public void exportProjectTable() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File csvFile = csvService.exportTable(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				"user"
		);
		assertTrue(csvFile.exists());
		assertTrue(csvFile.length() > 0);
		csvFile.deleteOnExit();
	}
	
	@Test
	public void exportQuery() throws Exception{
		//
		// First, import
		// 
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());

		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				null, 
				file, 
				true, 
				false);

		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);

		assertEquals(4, results.getNumberOfRowsInEntireTable());
		
		//
		// Now export
		//
		String sql = "SELECT * FROM assetsimportcompletesample WHERE age = ' 77'";
		File exportFile = CSVService.Factory.getInstance().exportQuery(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				sql
		);
		
		//
		// Now import - should be one row only
		//
		tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				null,
				exportFile, 
				true, 
				false);

		assertNotNull(tableData);
		
		results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable(tableData.tableName, null, false);
		
		assertEquals(1, results.getNumberOfRowsInEntireTable());
		
		//
		// Clean up
		//		
		new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				).runDBQuery("DROP TABLE " + tableData.tableName);
		
		exportFile.deleteOnExit();
	}
	
	@Test
	public void exportNonExistantProjectTable() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		try {
			csvService.exportTable(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					"banana");

			fail();
		} catch (Exception e) {
			// expected
		}
	}
	
	@Test
	public void exportEmptyProjectTable() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		try {
			csvService.exportTable(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					new TableData());

			fail();
		} catch (Exception e) {
			// expected
		}
	}
	
	@Test
	public void exportNullProjectTable() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		String tableName = null;
		try {
			csvService.exportTable(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					tableName);

			fail();
		} catch (Exception e) {
			// expected
		}
	}
	
	@Test
	public void exportNullProjectTableData() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		TableData tableName = null;
		try {
			csvService.exportTable(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					tableName);

			fail();
		} catch (Exception e) {
			// expected
		}
	}
	
	@Test
	public void exportProjectTableToFile() throws Exception{		
		TableData tableData = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				).getColumnNamesForTable("user");
		
		
        assertNotNull(tableData);
        
		CSVService csvService = CSVService.Factory.getInstance();
		File csvFile = csvService.exportTable(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				tableData);

		assertTrue(csvFile.exists());
		assertTrue(csvFile.length() > 0);
		csvFile.deleteOnExit();
	}
	
	@Test
	public void importTableFromFileWithCommasInHeader() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.Commas.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);

		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
	}
	
	/// HMMM!! Should ensure these are quoted.
	@Test
	public void importTableFromFileWithReservedWordsInHeader() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.Reserved.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		
		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
	}
	
	// HMMM!! Should ensure these are quoted.
	@Test
	public void importTableFromFileWithInvalidColumnsInHeader() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.InvalidColumnName.csv");
		assertTrue(file.exists());
		
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		
		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
	}
	
	@Test
	public void importTableFromFileWithEmptyColumnName() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.EmptyColumnName.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		assertNotNull(tableData);

		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
		
		boolean containsColumn_1 = false;
		for (OrdsTableColumn column : results.columns){
			if (column.columnName.equals("Column_1")) containsColumn_1 = true;
		}
		
		assertTrue(containsColumn_1);
	}
	
	@Test
	public void importTableFromFile() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		
		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
	}
	
	@Test
	public void importTableFromFileWithRequiredName() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				"banana",
				file, 
				true);		assertNotNull(tableData);
		assertEquals("banana", tableData.tableName);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("banana", null, false);		
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
				
		new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				).runDBQuery("DROP TABLE banana");
		
	}
	
	@Test
	public void importTwoTableFromFiles() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);		
		
		assertNotNull(tableData);
				
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);		
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
		
		//
		// Now another
		//		
		tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		
		assertNotNull(tableData);
		
		results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);		
		
		assertEquals(4, results.getNumberOfRowsInEntireTable());
		assertEquals("assetsimportcompletesample_2", tableData.tableName);
		
		new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				).runDBQuery("DROP TABLE assetsimportcompletesample_2");
	}
	
	@Test
	public void importTableFromFileNoHeaders() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				false);
		
		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(5, results.getNumberOfRowsInEntireTable());
	}
	
	@Test
	public void exportTableToFile() throws Exception{
		//
		// Import
		//
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		assertNotNull(tableData);
		
		//
		// Export
		//
		File csvFile = csvService.exportTable(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				tableData);

		String output = FileUtils.readFileToString(csvFile);
		assertTrue(output.contains("UserName,age,Height,DoB,Shoe size,Address"));
		csvFile.deleteOnExit();
		
	}
	
	
	@Test
	public void useGeneratedPrimaryKeyColumn() throws Exception{
		//
		// Import
		//
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		assertNotNull(tableData);
		
		//
		// Export
		//
		File csvFile = csvService.exportTable(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(), 
				tableData);

		String output = FileUtils.readFileToString(csvFile);
		assertTrue(output.contains("UserName,age,Height,DoB,Shoe size,Address,assetsimportcompletesample_index"));
		csvFile.deleteOnExit();
	}
	
	/**
	 * Full lifecycle test - import the data, export it to file, then create a new table by importing it again.
	 * @throws Exception
	 */
	@Test
	public void reimportTableFromFile() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		
		//
		// Import CSV as a new table
		//
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				file, 
				true);
		
		assertNotNull(tableData);
		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertNotNull(results);
		assertEquals(4, results.getNumberOfRowsInEntireTable());
		
		//
		// Export to CSV file
		//
		File csvFile = csvService.exportTable(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				tableData);

		String output = FileUtils.readFileToString(csvFile);
		assertTrue(output.contains("UserName,age,Height,DoB,Shoe size,Address"));

		//
		// Re-import from the file we just exported
		//
		
		// Get the normalised name for the table from the file
		String tableName = removeBadCharsFromName(csvFile.getName()).toLowerCase();
		
		tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				csvFile, 
				true);
		
		assertNotNull(tableData);
		
		results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable(tableName, null, false);
		
		assertNotNull(results);
		assertEquals(4, results.getNumberOfRowsInEntireTable());
		
		//
		// Delete the table
		//	
		new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				).runDBQuery("DROP TABLE " + tableName);
		
		csvFile.deleteOnExit();
	}
	
	@Test
	public void appendDataFromFile() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		assertTrue(file.exists());
		TableData tableData = csvService.newTableDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				null, 
				file, 
				true, 
				false);

		assertNotNull(tableData);
		
		csvService.appendDataFromFile(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
				"assetsimportcompletesample", 
				file);

		
		TableData results = new ORDSPostgresDB(
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
				ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName()
				)
		.getTableDataForTable("assetsimportcompletesample", null, false);
		
		assertEquals(8, results.getNumberOfRowsInEntireTable());
	}
	
	@Test
	public void importTableFromInvalidFile() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSampleInvalid.csv");
		assertTrue(file.exists());
		try {
			csvService.newTableDataFromFile(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					file, true);

			fail();
		} catch (Exception e) {
			// Expected
		}
	}
	
	@Test
	public void importTableFromMissingFile() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSampleInvalid_nosuchfile.csv");
		assertFalse(file.exists());
		try {
			csvService.newTableDataFromFile(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					file, 
					true);

			fail();
		} catch (Exception e) {
			// Expected
		}
	}
	
	@Test
	public void importTableFromNullFile() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = null;
		try {
			csvService.newTableDataFromFile(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					file, 
					true);

			fail();
		} catch (Exception e) {
			// Expected
		}
	}
	
	@Test
	public void appendDataFromFileToNonexistantDatabase() throws Exception{
		CSVService csvService = CSVService.Factory.getInstance();
		File file = new File("./src/test/resources/databases/AssetsImportCompleteSample.csv");
		try {
			csvService.appendDataFromFile(
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getHost(),
					ServerConfigurationService.Factory.getInstance().getOrdsDatabaseServer().getMasterDatabaseName(),
					"assetsimportcompletesample", 
					file);

			fail();
		} catch (Exception e) {
			// Expected
		}
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
