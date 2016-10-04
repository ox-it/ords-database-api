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

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Test;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;

import uk.ac.ox.it.ords.api.database.data.ImportProgress;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;

public class DatabaseTest extends AbstractDatabaseTestRunner{

	
	@Test ()
	public void uploadUnsupportedFile() throws FileNotFoundException {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");

		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		FileInputStream inputStream;
		
		File csvFile = new File(getClass().getResource("/config.xml").getFile());
		inputStream = new FileInputStream(csvFile);
		ContentDisposition cd = new ContentDisposition("attachment;filename=config.xml");
		Attachment att = new Attachment("databaseFile", inputStream, cd);
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
		Attachment att = new Attachment("databaseFile", inputStream, cd);
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
		Attachment att = new Attachment("databaseFile", inputStream, cd);
		client = getClient(false);
		client.type("multipart/form-data");
		Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
		assertEquals(403, response.getStatus());
	}
	
	@Test ()
	public void uploadCSVFile() {
		loginUsingSSO("pingu@nowhere.co","pingu@nowhere.co");
		
		WebClient client = getClient(false);
		client.type("multipart/form-data");
		
		File csvFile = new File(getClass().getResource("/small_test.csv").getFile());
		File accessFile = new File(getClass().getResource("/mondial.accdb").getFile());
		FileInputStream inputStream;
		try {
			
			// create a csv file database
			inputStream = new FileInputStream(csvFile);
			ContentDisposition cd = new ContentDisposition("attachment;filename=small_test.csv");
			Attachment att = new Attachment("databaseFile", inputStream, cd);

			Response response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			
			String id = getIdFromResponse(response);
			DatabaseReference r = new DatabaseReference(Integer.parseInt(id), false);
			AbstractResourceTest.databases.add(r);
			
			// get the data from the table created as small_test
			
			client = getClient(false);
			String tablePath = "/"+id+"/tabledata/small_test";
			response = client.path(tablePath).get();
			assertEquals(200, response.getStatus());
			TableData tableData = response.readEntity(TableData.class);
			//InputStream stream = (InputStream) response.getEntity();
			assertEquals(tableData.columns.size(), 6 );
			
			//this.getResponseFromInputStream(stream);
			
			// create an access database import
			inputStream = new FileInputStream(accessFile);
			cd = new ContentDisposition("attachement;filename=mondial.accdb");
			att = new Attachment("databaseFile", inputStream, cd);
			client = getClient(false);
			client.type("multipart/form-data");
			response = client.path("/"+logicalDatabaseId+"/data/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			
			id = getIdFromResponse(response);
			r = new DatabaseReference(Integer.parseInt(id), false);
			AbstractResourceTest.databases.add(r);
			
			// test view
			TableViewInfo viewInfo = new TableViewInfo();
			viewInfo.setViewQuery("SELECT CityName, Population from City");
			viewInfo.setViewName("Population City");
			viewInfo.setViewDescription("A view on the city table showing population and city name");
			viewInfo.setViewAuthorization("public");
			viewInfo.setViewTable("City");
			
			client = getClient(true);
			response = client.path("/"+id+"/dataset").post(viewInfo);
			assertEquals(201, response.getStatus());
			
			String viewPath = response.getLocation().getPath();
			String viewId = viewPath.substring(viewPath.lastIndexOf('/')+1);

			
			// view the view
			client = getClient(true);
			response = client.path("/"+id+"/dataset/"+viewId).get();
			assertEquals(200, response.getStatus());
			
			
			// update the view
			viewInfo.setViewName("The population of Cities");
			viewInfo.setViewAuthorization("authmembers");
			client = getClient(true);
			response = client.path("/"+id+"/dataset/"+viewId).put(viewInfo);
			assertEquals(200, response.getStatus());

			

			
			// delete the view
			client = getClient(true);
			response = client.path("/"+id+"/dataset/"+viewId).delete();
			assertEquals(200, response.getStatus());
			
			// test export
			
			client = getClient(true);
			client.accept("application/sql");
			String exportPath = "/"+id+"/export/sql";
			response = client.path(exportPath).get();
			assertEquals(200, response.getStatus());
			InputStream stream = (InputStream) response.getEntity();
			this.getResponseFromInputStream(stream, "/tmp/mondial.sql");
			
			//quick test for csv and zip export
			// TODO need to write a proper check for this
			client = getClient(true);
			client.accept("text/csv");
			response = client.path("/"+id+"/export/csv").get();
			assertEquals(200, response.getStatus());
			
			client = getClient(true);
			client.accept("application/zip");
			response = client.path("/"+id+"/export/zip").get();
			assertEquals(200, response.getStatus());
			
			
			
			
			
			
			client = getClient(false);
			response = client.path("/"+id+"/tabledata/country").get();
			assertEquals(200, response.getStatus());
			stream = (InputStream) response.getEntity();
			this.getResponseFromInputStream(stream, "mondial.json");
			//tableData = response.readEntity(TableData.class);
			/*
			File sqlFile = new File ("/tmp/mondial.sql");
			inputStream = new FileInputStream(sqlFile);
			cd = new ContentDisposition("attachement;filename=mondial.sql");
			att = new Attachment("databaseFile", inputStream, cd);
			client = getClient(false);
			client.type("multipart/form-data");
			response = client.path("/"+id+"/import/localhost").post(new MultipartBody(att));
			assertEquals(201, response.getStatus());
			id = getIdFromResponse(response);
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
*/
			
			//System.out.println("Number of Rows: "+tableData.getNumberOfRowsInEntireTable());
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
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
