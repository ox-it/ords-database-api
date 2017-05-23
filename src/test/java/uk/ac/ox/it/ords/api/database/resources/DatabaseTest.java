package uk.ac.ox.it.ords.api.database.resources;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javax.ws.rs.core.Response;

import org.junit.Test;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;

import uk.ac.ox.it.ords.api.database.data.ImportProgress;


public class DatabaseTest extends AbstractDatabaseTestRunner{

	
	@Test ()
	public void uploadUnsupportedFile() throws FileNotFoundException {
		loginBasicUser();

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		File csvFile = new File(getClass().getResource("/config.xml").getFile());
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=config.xml");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(415, response.getStatus());
	}
	
	
	@Test ()
	public void uploadCSVFileUnauth() throws FileNotFoundException {
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		// create a csv file database
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(403, response.getStatus());
	}
	
	@Test ()
	public void uploadAccessFileUnauth() throws FileNotFoundException {
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(403, response.getStatus());
	}
	
	@Test ()
	public void uploadSmallAccessFile() throws FileNotFoundException {
		loginBasicUser();

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());		

		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
	}
	
	
	@Test ()
	public void uploadSmallSQLFile() throws Exception {
		loginBasicUser();
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		File accessFile = new File(getClass().getResource("/databases/main_2812_2811.sql").getFile());
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=main_2812_2811.sql");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());	

		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);

	}
	
	@Test ()
	public void appendCSVToDatabase() throws FileNotFoundException {
		loginBasicUser();

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());

		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
	
		// try adding a csv
		client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		inputStream= new FileInputStream(csvFile);
		cd = new ContentDisposition("attachement;filename=small_test.csv");
		att = new Attachment("dataFile", inputStream, cd );
		response = client.path("/"+id+"/import/testTable/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String actualTableName = getIdFromResponse(response);
		System.out.println("New table name \"testTable\" wrangled to: \""+actualTableName+"\"");
	}
	
	
	@Test ()
	public void uploadSmallCSVFile() throws Exception {
		loginBasicUser();
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		FileInputStream inputStream;
			
		// create a csv file database
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
		Attachment att = new Attachment("dataFile", inputStream, cd);

		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		logout();
	}
	
	@Test ()
	public void uploadSmallMalformedCSVFile() throws Exception {
		loginBasicUser();
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/malformed_test.csv").getFile());
		FileInputStream inputStream;
			
		// create a csv file database
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=malformed_test.csv");
		Attachment att = new Attachment("dataFile", inputStream, cd);

		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(500, response.getStatus());
		
		logout();
	}
	
	@Test
	public void uploadLargeFileWithBasicAccount() throws Exception {
		loginBasicUser();

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		FileInputStream inputStream;
		
		File accessFile = new File(getClass().getResource("/databases/professions.accdb").getFile());
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=professions.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(403, response.getStatus());
		logout();
	}
	
//	@Test	
//	public void uploadLargeCSV() throws Exception {
//		this.loginUsingPremiumUser();
//
//		WebClient client = getClient(false);
//		client.type("multipart/form-data");
//		FileInputStream inputStream;
//		
//		File accessFile = new File(getClass().getResource("/databases/Postcode_LSOA_Part.csv").getFile());
//		inputStream = new FileInputStream(accessFile);
//		ContentDisposition cd = new ContentDisposition("attachement;filename=Postcode_LSOA_Part.csv");
//		Attachment att = new Attachment("dataFile", inputStream, cd);
//		client = getClient(false);
//		client.type("multipart/form-data");
//		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
//		assertEquals(202, response.getStatus());
//		
//		String id = response.readEntity(String.class);
//
//		// check import progress
//		
//		ImportProgress prg;
//		client = getClient(true);
//		response = client.path("/"+id+"/import").get();
//		assertEquals(200,response.getStatus());
//		prg = response.readEntity(ImportProgress.class);
//		while ( "QUEUED".equals(prg.getStatus())|| "IN_PROGRESS".equals(prg.getStatus())) {
//			System.out.println("Import Status: " + prg.getStatus());
//			client = getClient(true);
//			response = client.path("/"+id+"/import").get();
//			prg = response.readEntity(ImportProgress.class);
//			assertEquals(200,response.getStatus());				
//		}
//		assertEquals(prg.getStatus(), "FINISHED");
//
//		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
//		AbstractResourceTest.databases.add(r);
//
//		logout();
//	}
	
	@Test
	public void uploadLargeSQL() throws Exception {
		this.loginUsingPremiumUser();

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		FileInputStream inputStream;
		
		File accessFile = new File(getClass().getResource("/databases/postcodes.sql").getFile());
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachement;filename=postcodes.sql");
		Attachment att = new Attachment("dataFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(202, response.getStatus());
		
		String id = response.readEntity(String.class);

		// check import progress
		
		ImportProgress prg;
		client = getClient(true);
		response = client.path("/"+id+"/import").get();
		assertEquals(200,response.getStatus());
		prg = response.readEntity(ImportProgress.class);
		while ( "QUEUED".equals(prg.getStatus())|| "IN_PROGRESS".equals(prg.getStatus())) {
			System.out.println("Import Status: " + prg.getStatus());
			client = getClient(true);
			response = client.path("/"+id+"/import").get();
			prg = response.readEntity(ImportProgress.class);
			assertEquals(200,response.getStatus());				
		}
		assertEquals(prg.getStatus(), "FINISHED");

		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);

		logout();
		
	}
	
	
//	@Test
//	public void uploadLargeAccess() throws Exception {
//		this.loginUsingPremiumUser();
//
//		WebClient client = getClient(false);
//		client.type("multipart/form-data");
//		FileInputStream inputStream;
//		
//		File accessFile = new File(getClass().getResource("/databases/professions.accdb").getFile());
//		inputStream = new FileInputStream(accessFile);
//		ContentDisposition cd = new ContentDisposition("attachement;filename=professions.accdb");
//		Attachment att = new Attachment("dataFile", inputStream, cd);
//		client = getClient(false);
//		client.type("multipart/form-data");
//		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
//		assertEquals(202, response.getStatus());
//		
//		String id = response.readEntity(String.class);
//
//		// check import progress
//		
//		ImportProgress prg;
//		client = getClient(true);
//		response = client.path("/"+id+"/import").get();
//		assertEquals(200,response.getStatus());
//		prg = response.readEntity(ImportProgress.class);
//		while ( "QUEUED".equals(prg.getStatus())|| "IN_PROGRESS".equals(prg.getStatus())) {
//			System.out.println("Import Status: " + prg.getStatus());
//			client = getClient(true);
//			response = client.path("/"+id+"/import").get();
//			prg = response.readEntity(ImportProgress.class);
//			assertEquals(200,response.getStatus());				
//		}
//		assertEquals(prg.getStatus(), "FINISHED");
//
//		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
//		AbstractResourceTest.databases.add(r);
//
//		logout();
//		
//	}
	
	
	
	@Test
	public void exportSQL ( ) throws Exception {
		loginBasicUser();
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		FileInputStream inputStream;
			
		// create a csv file database
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
		Attachment att = new Attachment("dataFile", inputStream, cd);

		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		
		// test export
		
		client = getClient(true);
		client.accept("application/sql");
		String exportPath = "/"+id+"/export/sql";
		response = client.path(exportPath).get();
		assertEquals(200, response.getStatus());
		InputStream stream = (InputStream) response.getEntity();
		this.getResponseFromInputStream(stream, "/tmp/mondial.sql");

		logout();
	}
	
	
	@Test
	public void exportCSV ( ) throws Exception {
		loginBasicUser();
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream;
			
		// create a csv file database
		inputStream = new FileInputStream(accessFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=/mondial.accdb");
		Attachment att = new Attachment("dataFile", inputStream, cd);

		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);
		
		client = getClient(true);
		client.accept("text/csv");
		response = client.path("/"+id+"/export/csv").get();
		assertEquals(200, response.getStatus());
		
		client = getClient(true);
		client.accept("application/zip");
		response = client.path("/"+id+"/export/zip").get();
		assertEquals(200, response.getStatus());
		
		
		client = getClient(true);
		client.accept("text/csv");
		response = client.path("/"+id+"/export/table/city").get();
		assertEquals(200, response.getStatus());
	
		
		client = getClient(true);
		client.accept("text/csv");
		response = client.path("/"+id+"/export").query("q", "select \"Code\", \"CountryName\" from country").get();
		assertEquals(200, response.getStatus());
		InputStream stream = (InputStream) response.getEntity();
		//this.getResponseFromInputStream(stream, "/tmp/country.csv");
		
		client = getClient(false);
		response = client.path("/"+id+"/tabledata/country").get();
		assertEquals(200, response.getStatus());
		stream = (InputStream) response.getEntity();
		
		logout();
	}
	
	
	@Test
	public void exportUnauth ( ) throws Exception {
		loginBasicUser();
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		FileInputStream inputStream;
			
		// create a csv file database
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
		Attachment att = new Attachment("dataFile", inputStream, cd);

		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(201, response.getStatus());
		
		String id = getIdFromResponse(response);
		DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
		AbstractResourceTest.databases.add(r);	
		
		logout();
		
		client = getClient(true);
		client.accept("application/sql");
		String exportPath = "/"+id+"/export/sql";
		response = client.path(exportPath).get();
		assertEquals(403, response.getStatus());

	}
	
	
	private String getIdFromResponse( Response response ) {
		String path = response.getLocation().getPath();
		String id = path.substring(path.lastIndexOf('/')+1);
		return id;
	}
	
	
	private void getResponseFromInputStream(InputStream is, String fileName) {
        try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String next = null;
                PrintWriter writer = new PrintWriter(fileName);
                while ((next = reader.readLine()) != null) {
                        writer.println(next);
                }
                writer.close();
        } catch (IOException e) {
                e.printStackTrace();
        }
}



}
