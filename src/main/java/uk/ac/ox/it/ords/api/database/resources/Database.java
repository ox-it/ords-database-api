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


package uk.ac.ox.it.ords.api.database.resources;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.activation.DataHandler;
import javax.annotation.PostConstruct;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.Multipart;
import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.data.ColumnReference;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.TableView;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissions;
import uk.ac.ox.it.ords.api.database.services.DatabaseRecordService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.QueryService;
import uk.ac.ox.it.ords.api.database.services.SQLService;
import uk.ac.ox.it.ords.api.database.services.TableViewService;

@Path("/")
public class Database {

	@PostConstruct
	public void init() throws Exception {
		DatabaseUploadService.Factory.getInstance().init();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/datasetdata/{datasetid}")
	public Response getDatabaseDatasetData(@PathParam("id") final int id,
			@PathParam("datasetid") int datasetID,
			@DefaultValue("0") @QueryParam("startindex") int startIndex,
			@DefaultValue("50") @QueryParam("rowsperpage") int rowsPerPage) {

		TableData data = null;
		try {
			TableView tableView = tableViewService().getTableViewRecord(datasetID);
			if ( !"public".equals(tableView.getTvAuthorization())) {
				OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
				if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))){
					return Response.status(Response.Status.FORBIDDEN).build();
				}
			}
			data = tableViewService().getStaticDataSetData(id,
					datasetID, startIndex, rowsPerPage);
		} 
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.ok(data).build();
	}
	
	
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset/{datasetId}")
	public Response getDatabaseDataset(@PathParam("id") final int id,
			@PathParam("datasetId") int datasetId ) {
		TableViewInfo tableViewInfo = null;
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			tableViewInfo = tableViewService().getStaticDataSet(datasetId);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.ok(tableViewInfo).build();
		
		
	}
	
	

	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset/{datasetid}")
	public Response updateDatabaseDataset(@PathParam("id") final int id,
			@PathParam("datasetid") int datasetId,
			TableViewInfo tableViewInfo) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			tableViewService().updateStaticDataSet(id, datasetId, tableViewInfo);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.status(Response.Status.OK).build();
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset")
	public Response createDatabaseDataset(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			TableViewInfo tableViewInfo,
			@Context UriInfo uriInfo) {
		int staticDataSetId = 0;
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			staticDataSetId = tableViewService().createStaticDataSetOnQuery(id, tableViewInfo);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
	    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
	    builder.path(Integer.toString(staticDataSetId));
	    return Response.created(builder.build()).build();
	}
	
	

	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset/{datasetid}")
	public Response deleteDatabaseDataset(@PathParam("id") final int id,
			@PathParam("datasetid") int datasetID) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_DELETE(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			tableViewService().deleteStaticDataSet(id, datasetID);
		}
		catch ( NotFoundException nfe ) {
			return Response.status(Response.Status.GONE).build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
		return Response.status(Response.Status.OK).build();

	}

	/*
	 * @startindex=0
	 * 
	 * @rowsperpage=100
	 * 
	 * @filter
	 * 
	 * @filterparams
	 * 
	 * @sort
	 * 
	 * @direction
	 */
	///{startIndex}/{rowsPerPage}/{filter}/{sort}/{direction}
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/tabledata/{tablename}")
	public Response getTableData(@PathParam("id") String id,
			@PathParam("tablename") String tableName,
			@DefaultValue("0") @QueryParam("startIndex") int startIndex,
			@DefaultValue("100") @QueryParam("rowsPerPage") int rowsPerPage,
			@QueryParam("sort") String sort,
			@QueryParam("direction") String direction
			) {
		try {
			OrdsPhysicalDatabase physicalDatabase;
			int dbId;
			if ( isInt(id) ) {
				dbId = Integer.parseInt(id);
				physicalDatabase = databaseRecordService().getRecordFromId(dbId);
			}
			else {
				physicalDatabase = databaseRecordService().getRecordFromGivenName(id);
				dbId = physicalDatabase.getPhysicalDatabaseId();
			}
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			TableData tableData = null;

			if ( "none".equalsIgnoreCase(sort)) {
				sort = null;
			}
			if ( "none".equalsIgnoreCase(direction)) {
				direction = null;
			}
			tableData = tableViewService().getDatabaseRows(dbId, tableName, startIndex, rowsPerPage, sort, direction);
			return Response.status(Response.Status.OK).entity(tableData).build();
		}
		catch (BadParameterException ex) {
			return Response.status(Response.Status.NOT_FOUND).entity(ex)
					.build();
		} 
		catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}		

	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{id}/{instance}/tabledata/{tablename}")
	public Response appendTableData(@PathParam("id") final int id,
			@PathParam("tablename") String tableName, Row newData) {
		
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			tableViewService().appendTableData(id, tableName, newData);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex)
					.build();
		} 
		catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.status(Response.Status.CREATED).build();

	}

	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{id}/tabledata/{tablename}")
	public Response updateTableRow(@PathParam("id") final int id,
			@PathParam("tablename") String tableName,
			List<Row> rowData) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			tableViewService().updateTableRow(id, tableName, rowData);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex)
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.status(Response.Status.OK).build();

	}
	

	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{id}/tabledata/{tablename}")
	public Response deleteTableRow(
			@PathParam("id") final int id,
			@PathParam("tablename") String tableName,
			@QueryParam("primaryKey") String primaryKey,
			@QueryParam("primaryKeyValue") String primaryKeyValue
			) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			tableViewService().deleteTableData(id, tableName, primaryKey, primaryKeyValue);
		}
		catch ( NotFoundException nfe ) {
			return Response.status(Response.Status.GONE).build();
		}
		catch ( Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
		return Response.status(Response.Status.OK).build();

	}

	/*
	 * @referencedtablecolumn
	 * 
	 * @startindex=0
	 * 
	 * @rowsperpage=100
	 * 
	 * @filter
	 * 
	 * @filterparams
	 * 
	 * @sort
	 * 
	 * @direction
	 * 
	 * @query don't need these
	 * 
	 * need to return triples (id/value/label)
	 */

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/table/{tablename}/column/{foreignkeycolumn}/related/")
	public Response getReferencedColumns(@PathParam("id") final int id,
			@PathParam("tablename") String tableName,
			@PathParam("foreignkeycolumn") String foreignKeyColumn,
			@QueryParam("term") String term) {

		TableData data = null;
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			 data = queryService().getReferenceColumnData(id, tableName, foreignKeyColumn, term);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex.getMessage())
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		
		return Response.status(Response.Status.OK).entity(data).build();
	}

	/*
	 * @q
	 * 
	 * @startindex=0
	 * 
	 * @rowsperpage=100
	 * 
	 * @filter
	 * 
	 * @order
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/query")
	public Response doQuery(@PathParam("id") final int id,
			@QueryParam("q") String theQuery,
			@DefaultValue("0") @QueryParam("startindex") int startIndex,
			@DefaultValue("50") @QueryParam("rowsperpage") int rowsPerPage) {
		TableData data = null;
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			data = queryService().performQuery(id, theQuery, startIndex, rowsPerPage, null, null);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex)
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.status(Response.Status.OK).entity(data).build();

	}

	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/{instance}/data")
	public Response exportDataBase(@PathParam("id") final int id) {
		String sql = "";
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			sql = SQLService.Factory.getInstance().buildSQLExportForDatabase(id);
		}
		catch (BadParameterException ex) {
			Response.status(Response.Status.NOT_FOUND).entity(ex)
					.build();
		} 
		catch (Exception e) {
			Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.entity(e).build();
		}
		return Response.status(Response.Status.OK).entity(sql).build();
	}
	

	@POST
	@Path("{id}/data/{server}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response handleFileUpload(
			@PathParam("id") int dbId,
			@PathParam("server") String server,
			@Multipart("databaseFile") Attachment fileAttachment,
			@Context ServletContext context,
			@Context UriInfo uriInfo) {
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_CREATE)) {
			return Response.status(Response.Status.FORBIDDEN).build();
		}
		//MediaType contentType = fileAttachment.getContentType();
		//contentType.
		int newDbId = 0;
		MultivaluedMap<String, String> map = fileAttachment.getHeaders();
		String fileName = getFileName(map);
		String extension = getFileExtension(fileName);
		if ( extension == null || (!extension.equalsIgnoreCase("csv") &&
				!extension.equalsIgnoreCase("mdb") &&
				!extension.equalsIgnoreCase("accdb"))) {
			return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE).build();
		}
		File tempDirectory;
		if ( context == null ) {
			tempDirectory = new File("/tmp");
		}
		else {
			tempDirectory = (File)context.getAttribute("javax.servlet.context.tmpdir");
		}
		if (tempDirectory == null ) {
			// try with tmp again
			tempDirectory = new File("/tmp");
		}
		File dbFile = new File(tempDirectory, fileName);
		
		DataHandler handler = fileAttachment.getDataHandler();
		try {
			InputStream stream = handler.getInputStream();
			saveFile(dbFile, stream);
			newDbId = DatabaseUploadService.Factory.getInstance().createNewDatabaseFromFile(dbId, dbFile, extension, server);		}
		catch (Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
	    builder.path(Integer.toString(newDbId));
	    return Response.created(builder.build()).build();
	}
	
	
	// convenience function to remove database for tests, this will go when the relationship between this
	// microservice and database structure api microservice is properly configured
	
	@DELETE
	@Path("{id}/test/delete/{staging}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response testDeleteDatabase(
			@PathParam("id") int dbId,
			@PathParam("staging") BooleanCheck staging) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(dbId);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_DELETE(physicalDatabase.getLogicalDatabaseId()))) {
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			DatabaseUploadService.Factory.getInstance().testDeleteDatabase(dbId, staging.value);
			return Response.ok().build();
		}
		catch (Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}

				
	}

	private QueryService queryService() {
		return QueryService.Factory.getInstance();
	}

	private TableViewService tableViewService() {
		return TableViewService.Factory.getInstance();
	}
	
	private DatabaseRecordService databaseRecordService() {
		return DatabaseRecordService.Factory.getInstance();
	}
	
	
	private boolean isInt ( String intString ) {
		try {
			int i = Integer.parseInt(intString);
			return true;
		}
		catch (NumberFormatException e ) {
			return false;
		}
	}
	
	
    private String getFileName(MultivaluedMap<String, String> header) {
    	 
        String[] contentDisposition = header.getFirst("Content-Disposition").split(";");
 
        for (String filename : contentDisposition) {
            if ((filename.trim().startsWith("filename"))) {
 
                String[] name = filename.split("=");
 
                String finalFileName = name[1].trim().replaceAll("\"", "");
                return finalFileName;
            }
        }
        return "unknown";
    }

    private String getFileExtension(String fileName ) {
    	if ( fileName.lastIndexOf('.') == -1 ) {
    		return null;
    	}
    	return fileName.substring(fileName.lastIndexOf('.')+1);
    }
    
    
    private void saveFile ( File f, InputStream stream ) throws Exception {
        OutputStream out = new FileOutputStream(f);
        
        int read = 0;
        byte[] bytes = new byte[1024];
        while ((read = stream.read(bytes)) != -1) {
            out.write(bytes, 0, read);
        }
        stream.close();
        out.flush();
        out.close();

    }
    
 
	// checks for a number of possible permutations for the staging part of the resource path
	public static class BooleanCheck {
		private static final BooleanCheck FALSE = new BooleanCheck(false);
		private static final BooleanCheck TRUE = new BooleanCheck(true);
		private boolean value;

		private BooleanCheck(boolean value) {
			this.value = value;
		}

		public boolean getValue() {
			return this.value;
		}

		public static BooleanCheck valueOf(String value) {
			switch (value.toLowerCase()) {
				case "true" :
				case "yes" :
				case "y" :
				case "staging" : {
					return BooleanCheck.TRUE;
				}
				default : {
					return BooleanCheck.FALSE;
				}
			}
		}
	}

 }
