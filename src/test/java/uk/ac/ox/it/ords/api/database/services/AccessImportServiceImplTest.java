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

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.ox.it.ords.api.database.data.DataRow;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.queries.GeneralSQLQueries;
import uk.ac.ox.it.ords.security.configuration.MetaConfiguration;

public class AccessImportServiceImplTest {
	
	Configuration properties = MetaConfiguration.getConfiguration();

	protected static String ORDS_DATABASE_NAME = "ords.database.name";
	protected static String ORDS_DATABASE_HOST = "ords.database.server.host";
	
	@Test
	public void preflightNull() throws Exception {
		assertFalse(AccessImportService.Factory.getInstance().preflightImport(null));
	}
	
	@Test
	public void createSchemaNull() throws Exception {
		Map<String, TableImportResult> schemaResult = AccessImportService.Factory.getInstance().createSchema(
				properties.getString(ORDS_DATABASE_HOST),
				properties.getString(ORDS_DATABASE_NAME),
				null);
		assertNull(schemaResult);
	}
	
	@Test
	public void preflightNoFile() throws Exception {
		assertFalse(AccessImportService.Factory.getInstance().preflightImport(new File("banana")));
	}
	
	@Test
	public void createSchemaNoFile() throws Exception {
		Map<String, TableImportResult> schemaResult = AccessImportService.Factory.getInstance().createSchema(
				properties.getString(ORDS_DATABASE_HOST),
				properties.getString(ORDS_DATABASE_NAME),
				new File("banana"));
		assertNull(schemaResult);
	}
	
	@Test
	@Ignore // this takes ages - useful to run sometimes, but not for every build
	public void importCOEL() throws Exception{
		String[] tableNames = {"status","relationships","names","provenanceanalysis","sourceedition","familytrees","person","tenantsinchief","provenance","tablecount","errors","sources","predecesnew","source","bibliography","hierarchylink","ticklines","sourcedata","layouts","tickheader","personsources","counties","users","licenses","personnamelink","statistics","reportentrynos","locklog","personanalysissource","willypics","sourcetext","companylicense","reports","publishstructure","personanalysis","domesday","years","system","myrelations","domesdaydata"};
		String dbFileName = "COEL_refsddstripped.accdb";
		// we know one table has duplicate primary keys
		importDatabase(dbFileName, tableNames,0,1);
	}
	
	@Test
	public void importInvoices2() throws Exception{
		String[] tableNames = {"tblinv","tblinvdetail","tblcust","tblprod","tblrep","tblstate"};
		String dbFileName = "invoice2.mdb";
		importDatabase(dbFileName, tableNames,0,0);
	}
	
	@Test
	public void importProfessions() throws Exception{
		String[] tableNames = {"census", "directories", "documents", "lookup_project_role", "lookup_repository",  "marriages", "marriages_persons_link", "occupation_codes","occupation_stats_1851", "occupations_evidence","person_doc_id","persons"};
		String dbFileName = "professions.accdb";
		importDatabase(dbFileName, tableNames,0,0);
	}
	
	@Test
	@Ignore // this takes ages - useful to run sometimes, but not for every build
	public void importPip() throws Exception{
		String[] tableNames = {"tblbequestrecipienttype", "lookupbequestreciptype", "tblburial", "tblrelats", "tblhearthtax", "tblproplease", "lookupproptypes", "tblsubsidy", "tblproproom", "tblbmbindivjunction", "tblhousedestr", "tblindividualstatus", "lookupparishref", "lookupstatus", "tbltithes", "tblindividualagemaritalstatus", "tblprop", "tblbmbevent", "tblpropeventtype", "lookupinstitutiontype", "tbldocument", "tblparishlists", "tblinstitutiontype", "lookupforenames", "tbl1695", "lookuppropeventtype", "lookupcounty", "lookuprelat", "tblindividualinstitution", "lookupagemaritalstatus", "lookupindividualrole", "tblpoll", "tblwindow", "lookupdocumentlanguage", "tblbequesttype", "tblindividualrole", "tblpropeventjunction", "lookuparchive", "lookupbequesttype", "tblbequestmodetype", "tblaids", "lookupdocumenttype", "tblbequestdetail", "tblpropindiv", "tblbequestdescription", "tblbequestitem", "tblbmbrelatjunction", "tblbequestvalue", "lookupitemtype", "lookupbequestmodetype", "lookuppropindivroletype"};
		String dbFileName = "pip_db.mdb";
		importDatabase(dbFileName, tableNames,0,0);
	}
	
	@Test
	@Ignore // this takes ages - useful to run sometimes, but not for every build
	public void importTfp() throws Exception{
		String[] tableNames = {"temp3","sequences","tblall_blast_results","tbl_temp","tblgels","tblmsdata","chlamy","dros","frm_temp","tbl_all_recip","tblbands","tblsearchtemp","tblsearch","tbl_proteome"};
		String dbFileName = "TbFP.mdb";
		importDatabase(dbFileName, tableNames,0,0);
	}
	
	public Map<String, TableImportResult> importDatabase(String dbFileName, String[] tableNames, int expectedCreateErrors, int expectedImportErrors) throws Exception{
		
		File file = new File("./src/test/resources/databases/"+dbFileName);
		assertTrue(file.exists());
		AccessImportService svc = AccessImportService.Factory.getInstance();
		
		System.out.println("==================");
		System.out.println(dbFileName);
		System.out.println("==================");

		//
		// Preflight
		//
		assertTrue(svc.preflightImport(file));

		boolean success = true;
		Map<String, TableImportResult> results = null;

		try {
			//
			// Create schema
			// 
			Map<String, TableImportResult> schemaResult = svc.createSchema(
					properties.getString(ORDS_DATABASE_HOST),
					properties.getString(ORDS_DATABASE_NAME),
					file);
			
			//
			// Check we get any expected errors on db create
			//
			int errors = 0;
			for (String key: schemaResult.keySet()){
				if (schemaResult.get(key).getTableCreateResult() == TableImportResult.FAILED) errors++;
			}
			assertEquals(expectedCreateErrors, errors);
			
			//
			// Check that we have the right tables created in the DB
			//			
			
			TableData tableData = new GeneralSQLQueries(
					properties.getString(ORDS_DATABASE_HOST),
					properties.getString(ORDS_DATABASE_NAME)
					)
			.getTableDataForDatabase();
			
			Set<String> tables = new HashSet<String>();
			for (DataRow dr : tableData.rows) { // Look through each table name in the database
				for (String s : dr.cell.keySet()) {
					tables.add( dr.cell.get(s).getValue());
				}
			}
			for (String tableName : tableNames){
				assertTrue(tables.contains(tableName));
			}
			
			//
			// Import the data
			//
			results = svc.importData(
					properties.getString(ORDS_DATABASE_HOST),
					properties.getString(ORDS_DATABASE_NAME), 
					file);
			assertEquals(tableNames.length, results.size());
			
			//
			// Do some analysis
			//
			errors = 0;
			for (String tableName: results.keySet()){
				// Merge in schema creation results
				results.get(tableName).merge(schemaResult.get(tableName));
				
				// Output some handy data
				System.out.println("---------------");
				System.out.println(tableName);
				System.out.println(results.get(tableName).toString());
				
				// Add up import errors
				if (results.get(tableName).getDataImportResult() == TableImportResult.FAILED) errors++;
			}
			
			//
			// Check we have the right number of errors in imports
			//
			assertEquals(expectedImportErrors, errors);
			

		} catch (Exception e) {
			success = false;
			e.printStackTrace();
		} finally {

			//
			// Clean up
			//
			String sql = "DROP TABLE ";
			for (String tableName: tableNames){
				sql += "\""+tableName+"\",";
			}
			sql = sql.substring(0, sql.lastIndexOf(","));
			
			new GeneralSQLQueries(
					properties.getString(ORDS_DATABASE_HOST),
					properties.getString(ORDS_DATABASE_NAME)
					).runDBQuery(sql);
		}
		if (!success) fail();
	    return results;
	}

	
	@Test
	public void importInvoices() throws Exception{
		String[] tableNames = {"tblinv","tblinvdetail","tblcust","tblprod","tblrep","tblstate"};
		String dbFileName = "invoice.mdb";
		Map<String, TableImportResult> results = importDatabase(dbFileName, tableNames,0,0);
		assertEquals(75, results.get("tblinv").getRowsVerified());
		assertEquals(151, results.get("tblinvdetail").getRowsVerified());
		assertEquals(4, results.get("tblcust").getRowsVerified());
		assertEquals(8, results.get("tblprod").getRowsVerified());
		assertEquals(2, results.get("tblrep").getRowsVerified());
		assertEquals(56, results.get("tblstate").getRowsVerified());
	}
	
	@Test
	public void importRachel() throws Exception{
		String[] tableNames = {"cars","drivers","houses","people"};
		String dbFileName = "Rachel_db3.accdb";
		Map<String, TableImportResult> results = importDatabase(dbFileName, tableNames,0,0);
		assertEquals(2, results.get("cars").getRowsVerified());
		assertEquals(4, results.get("drivers").getRowsVerified());
		assertEquals(3, results.get("houses").getRowsVerified());
		assertEquals(7, results.get("people").getRowsVerified());
		assertEquals(4,  results.get("cars").getConstraintsAdded()
				+ results.get("drivers").getConstraintsAdded()
				+ results.get("houses").getConstraintsAdded()
				+ results.get("people").getConstraintsAdded());
	}

}
