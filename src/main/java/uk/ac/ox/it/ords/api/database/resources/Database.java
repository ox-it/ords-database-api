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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.io.File;
import java.io.InputStream;
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
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.Multipart;
import org.apache.log4j.Logger;
import org.apache.shiro.SecurityUtils;

import uk.ac.ox.it.ords.api.database.data.ImportProgress;
import uk.ac.ox.it.ords.api.database.data.Row;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.data.TableViewInfo;
import uk.ac.ox.it.ords.api.database.exceptions.BadParameterException;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.model.TableView;
import uk.ac.ox.it.ords.api.database.permissions.DatabasePermissions;
import uk.ac.ox.it.ords.api.database.services.CSVService;
import uk.ac.ox.it.ords.api.database.services.DatabaseAuditService;
import uk.ac.ox.it.ords.api.database.services.DatabaseRecordService;
import uk.ac.ox.it.ords.api.database.services.DatabaseUploadService;
import uk.ac.ox.it.ords.api.database.services.QueryService;
import uk.ac.ox.it.ords.api.database.services.SQLService;
import uk.ac.ox.it.ords.api.database.services.TableViewService;
import uk.ac.ox.it.ords.api.database.utils.FileUtilities;

@Api(value="Database")
@Path("/")
public class Database {
	
	static Logger log = Logger.getLogger(Database.class);

	@PostConstruct
	public void init() throws Exception {
		DatabaseUploadService.Factory.getInstance().init();
	}
	
	@ApiOperation(
		value="Gets the data for a specified dataset",
		notes="Optional query parameters start and length can be used to paginate over the dataset",
		response=uk.ac.ox.it.ords.api.database.data.TableData.class
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Dataset successfully queried."),
		@ApiResponse(code = 404, message = "The static dataset does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to access the dataset.")
	})
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/datasetdata/{datasetid}")
	public Response getDatabaseDatasetData(@PathParam("id") final int id,
			@PathParam("datasetid") int datasetID,
			@DefaultValue("0") @QueryParam("startindex") int startIndex,
			@DefaultValue("50") @QueryParam("rowsperpage") int rowsPerPage,
			@QueryParam("sort") String sort,
			@QueryParam("direction") String direction ) {
		
		TableView tableView = tableViewService().getTableViewRecord(datasetID);
		if (tableView == null) return Response.status(404).build();
		
		//
		// Check permissions
		//
		if ( !"public".equals(tableView.getTvAuthorization())) {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))){
				
				DatabaseAuditService.Factory.getInstance().createNotAuthRecord("GET " + id + "/datasetdata/"+datasetID, id);
				
				return Response.status(Response.Status.FORBIDDEN).build();
			}
		}
		
		try {
			TableData data = tableViewService().getStaticDataSetData(id,
					datasetID, startIndex, rowsPerPage, sort, direction);
			
			return Response.ok(data).build();
		} 
		catch  (Exception e) {
			
			log.error(e);
						
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}

	}

	@ApiOperation(
			value="Gets the meta data for matching public datasets",
			notes="This returns the data transfer objects used to create the dataset",
			response=uk.ac.ox.it.ords.api.database.data.TableViewInfo.class
	)
	@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Query results retrieved."),
	})
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/dataset/")
	public Response searchPublicDatasets(
			@QueryParam("q") String term
			) throws Exception
	{
		
		List<TableViewInfo> datasets;
		
		datasets = TableViewService.Factory.getInstance().searchDataSets(term);
		
		return Response.ok(datasets).build();
	}
	
	@ApiOperation(
			value="Gets the meta data for a specified dataset",
			notes="This returns the data transfer object used to create the dataset",
			response=uk.ac.ox.it.ords.api.database.data.TableViewInfo.class
	)
	@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Dataset metadata successfully retrieved."),
		    @ApiResponse(code = 404, message = "The dataset does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to access the dataset.")
	})
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset/{datasetId}")
	public Response getDatabaseDataset(@PathParam("id") final int id,
			@PathParam("datasetId") int datasetId ) {

		//
		// Obtain the database
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();

		//
		// Check permissions
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("GET " + id + "/dataset/" + datasetId, id);

			return Response.status(Response.Status.FORBIDDEN).build();
		}

		//
		// Attempt to get the dataset
		//
		try {
			TableViewInfo tableViewInfo = tableViewService().getStaticDataSet(datasetId);

			DatabaseAuditService.Factory.getInstance().createDataQueryRecord(id);

			return Response.ok(tableViewInfo).build();

		} catch (NotFoundException e) {

			return Response.status(404).build();

		} catch (Exception e) {
			
			log.error(e);

			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}

	}
	
	
	
	@ApiOperation(
		value="Updates the data and metadata for a specified dataset"
	)
	@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Dataset successfully queried."),
		    @ApiResponse(code = 404, message = "The dataset does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to modify the dataset.")
	})
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset/{datasetid}")
	public Response updateDatabaseDataset(@PathParam("id") final int id,
			@PathParam("datasetid") int datasetId,
			TableViewInfo tableViewInfo) {

		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();

		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("PUT " + id + "/dataset/"+datasetId, id);
			
			return Response.status(Response.Status.FORBIDDEN).build();
		}


		try {
			
			tableViewService().updateStaticDataSet(id, datasetId, tableViewInfo);	
			
			DatabaseAuditService.Factory.getInstance().createDataChangeRecord(id);
			
			return Response.status(Response.Status.OK).build();

		}catch (Exception e) {
			
			log.error(e);
			
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		
	}
	
	

	@ApiOperation(
		value="Creates a new dataset based upon the specified dataset",
		notes="This appends the id for the new dataset to the url"
	)
	@ApiResponses(value = { 
			@ApiResponse(code = 201, message = "Dataset successfully created."),
		    @ApiResponse(code = 404, message = "Original database does not exist."),
		    @ApiResponse(code = 403, message = "Not authorized to access the dataset.")
	})
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset")
	public Response createDatabaseDataset(@PathParam("id") final int id,
			@PathParam("instance") String instance,
			TableViewInfo tableViewInfo,
			@Context UriInfo uriInfo) {
		
		//
		// Get database
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		
		if (physicalDatabase == null){
			return Response.status(Response.Status.NOT_FOUND).build();
		} 
		
		//
		// Check permission
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {

			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("POST " + id + "/dataset/", id);

			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		//
		// Perform validations
		//
		// TODO more validations here please!
		if (tableViewInfo == null){
			return Response.status(400).build();
		}
		
		//
		// Create dataset
		//
		try {
			
			int staticDataSetId = tableViewService().createStaticDataSetOnQuery(id, tableViewInfo);
		    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
		    builder.path(Integer.toString(staticDataSetId));
		   
			DatabaseAuditService.Factory.getInstance().createDatasetCreateRecord(id, staticDataSetId);

		    return Response.created(builder.build()).build();
		    
		} 	catch (Exception e) {
			
			log.error(e);

			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		

	}
	
	
	@ApiOperation(
		value="Deletes a dataset"
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Dataset successfully deleted."),
	    @ApiResponse(code = 404, message = "Dataset does not exist."),
	    @ApiResponse(code = 403, message = "Not authorized to delete the dataset.")
	})
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/dataset/{datasetid}")
	public Response deleteDatabaseDataset(@PathParam("id") final int id,
			@PathParam("datasetid") int datasetID) {

		//
		// Get database
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();
		
		//
		// Check permission
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_DELETE(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("DELETE " + id + "/dataset/"+datasetID, id);

			return Response.status(Response.Status.FORBIDDEN).build();
		}
		try {
			tableViewService().deleteStaticDataSet(datasetID);

			DatabaseAuditService.Factory.getInstance().createDatasetDeleteRecord(id, datasetID);

			return Response.status(Response.Status.OK).build();

		}
		
		catch (NotFoundException ex) {
			
			log.error(ex);
			
			return Response.status(Response.Status.NOT_FOUND).build();
		} 

		catch ( Exception e ) {

			log.error(e);

			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	}
	


	@ApiOperation(
		value="Gets table data",
		notes="Database id and table name are in the path. There are four optional query parameters for start, length, sort and direction",
		response=uk.ac.ox.it.ords.api.database.data.TableData.class
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Table data successfully returned."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to access the database.")
	})
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/tabledata/{tablename}")
	public Response getTableData(@PathParam("id") String id,
			@PathParam("tablename") String tableName,
			@DefaultValue("0") @QueryParam("start") int startIndex,
			@DefaultValue("100") @QueryParam("length") int rowsPerPage,
			@QueryParam("sort") String sort,
			@QueryParam("direction") String direction
			) {
		
		int dbId;
		OrdsPhysicalDatabase physicalDatabase = null; 

		//
		// Obtain the database
		//
		try {
			if ( isInt(id) ) {
				dbId = Integer.parseInt(id);
				physicalDatabase = databaseRecordService().getRecordFromId(dbId);
			}
			else {
				physicalDatabase = databaseRecordService().getRecordFromGivenName(id);
				dbId = physicalDatabase.getPhysicalDatabaseId();
			}
		} catch (Exception ex) {
			return Response.status(Response.Status.NOT_FOUND).build();
		} 

		if (physicalDatabase == null){
			return Response.status(404).build();
		}

		//
		// Check permission
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("GET " + id + "/tabledata/" + tableName, dbId);

			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		//
		// Set defaults
		//

		if ( "none".equalsIgnoreCase(sort)) {
			sort = null;
		}
		if ( "none".equalsIgnoreCase(direction)) {
			direction = null;
		}

		try {
			
			//
			// Get data
			//
			TableData tableData = tableViewService().getDatabaseRows(dbId, tableName, startIndex, rowsPerPage, sort, direction);

			//
			// Validate: TODO we should be able to check this before executing the request?
			//
			if (tableData == null){
				return Response.status(404).build();
			}
			
			return Response.status(Response.Status.OK).entity(tableData).build();
			
		} catch (Exception e) {

			log.error(e);
			
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}		
	}
	
	

	@ApiOperation(
		value="Appends table data",
		notes="A row of data is added to the database table"
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Table data successfully appended."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to modify the database.")
	})
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{id}/tabledata/{tablename}")
	public Response appendTableData(@PathParam("id") final int id,
			@PathParam("tablename") String tableName, Row newData) {
		
		//
		// Get the database
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();
		
		//
		// Check permissions
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
						
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("POST " + id + "/tabledata/" + tableName, id);
			
			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		//
		// Validate
		//
		if (newData == null) return Response.status(400).build();
		
		//
		// Append the data
		//
		try {
			 if (!tableViewService().appendTableData(id, tableName, newData)){
				 
				 //
				 // If the method returns false, there was a problem with the input
				 //
				 return Response.status(400).build();
			 };
			 
		} catch (Exception e) {
			
			log.error(e);
						
			return Response.status(500).build();
		}
		
		DatabaseAuditService.Factory.getInstance().createDataInsertRecord(id);
		
		return Response.status(Response.Status.CREATED).build();
	}
	
	
	
	@ApiOperation(
		value="Updates table data",
		notes="The row list must have a valid lookup column and value for each row"
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Table data successfully updated."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to modify the database.")
	})
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{id}/tabledata/{tablename}")
	public Response updateTableRow(@PathParam("id") final int id,
			@PathParam("tablename") String tableName,
			List<Row> rowData) {
				
		//
		// Check the database exists
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);

		if (physicalDatabase == null){
			return Response.status(Response.Status.NOT_FOUND).build();
		}
		
		//
		// Check permissions
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("PUT " + id + "/tabledata/" + tableName, id);
	
			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		//
		// Validate input TODO is this all?
		//
		if (rowData == null){
			return Response.status(400).build();
		}
		
		//
		// Perform the update
		//
		try {
			
			tableViewService().updateTableRow(id, tableName, rowData);
			
			DatabaseAuditService.Factory.getInstance().createDataChangeRecord(id);
			
		} catch (BadParameterException ex) {
			
			log.error(ex);
				
			return Response.status(400).build();
			
		} catch (NotFoundException ex) {
			
			log.error(ex);
				
			return Response.status(Response.Status.NOT_FOUND).build();
			
		} catch (Exception e) {
						
			log.error(e);
			
			// TODO this can be thrown by what are really 400 errors - e.g. missing lookupColumn and lookupValue - catch these before getting here
			
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		
		return Response.status(Response.Status.OK).build();
	}
	
	
	
	@ApiOperation(
		value="Deletes a single row from the table"
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Table row successfully deleted."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to modify the database.")
	})
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
		
		//
		// Check database exists
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();
		
		//
		// Check permissions
		// 
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("DELETE " + id + "/tabledata/" + tableName, id);
			
			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		//
		// Perform delete
		//
		try {
			
			// TODO check table actually exists
			// TODO check row actually exists
			
			tableViewService().deleteTableData(id, tableName, primaryKey, primaryKeyValue);
			
			DatabaseAuditService.Factory.getInstance().createDataDeleteRecord(id);
			
			return Response.status(Response.Status.OK).build();

		} catch (BadParameterException ex) {
				
			log.error(ex);
				
			return Response.status(Response.Status.NOT_FOUND).build();
				
		} catch (Exception e) {
			
			log.error(e);
			
			return Response.status(500).build();
			
		}
	}
	
	

	@ApiOperation(
		value="Gets data from the related table for a foreign key",
		notes="The row list must have a valid lookup column and value for each row",
		response=uk.ac.ox.it.ords.api.database.data.TableData.class
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Table data successfully updated."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to modify the database.")
	})
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/table/{tablename}/column/{foreignkeycolumn}/related/")
	public Response getReferencedColumns(@PathParam("id") final int id,
			@PathParam("tablename") String tableName,
			@PathParam("foreignkeycolumn") String foreignKeyColumn,
			@QueryParam("term") String term) {

		//
		// Get database
		//
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();
		
		//
		// Check permissions
		//
		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("GET " + id + "/table/" + tableName +"/column/" + foreignKeyColumn + "/related", id);
			
			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		try {
			TableData data = queryService().getReferenceColumnData(id, tableName, foreignKeyColumn, term);
			
			return Response.status(Response.Status.OK).entity(data).build();

		}
		catch (Exception e) {
			
			log.error(e);
			
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		
	}


	@ApiOperation(
		value="Runs a query against the database",
		notes="",
		response=uk.ac.ox.it.ords.api.database.data.TableData.class
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Query successfully run."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to access the database.")
	})
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{id}/query")
	public Response doQuery(@PathParam("id") final int id,
			@QueryParam("q") String theQuery,
			@DefaultValue("0") @QueryParam("start") int startIndex,
			@DefaultValue("100") @QueryParam("length") int rowsPerPage) {
		TableData data = null;
		
		OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
		if (physicalDatabase == null) return Response.status(404).build();

		if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("GET" + id + "/query/", id);
			
			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		try {
			data = queryService().performQuery(id, theQuery, startIndex, rowsPerPage, null, null);
			
			DatabaseAuditService.Factory.getInstance().createDataQueryRecord(id);
			
			return Response.status(Response.Status.OK).entity(data).build();
		}
		//
		// TODO as far as I can tell this is never thrown - it should whenever the query
		// has any problems with it, rather than falling over and generating a 500.
		//
		catch (BadParameterException ex) {
			
			log.error(ex);
			
			return Response.status(Response.Status.BAD_REQUEST).build();
		} 
		catch (Exception e) {
			
			log.error(e);
			
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}

	}

	@ApiOperation(
		value="Exports the database as sql, single csv or zipped csvs",
		notes="",
		response=String.class
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 200, message = "Export successful."),
		@ApiResponse(code = 404, message = "Database does not exist."),
		@ApiResponse(code = 403, message = "Not authorized to modify the database.")
	})
	@GET
	@Path("{id}/export/{type}")
	public Response exportDatabase(@PathParam("id") final int id,
			@PathParam("type") String exportType ) {
		File output = null;
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(id);
			
			if (!SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_MODIFY(physicalDatabase.getLogicalDatabaseId()))) {
				
				DatabaseAuditService.Factory.getInstance().createNotAuthRecord("GET " + id + "/data/", id);
				
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			if ( "sql".equalsIgnoreCase(exportType) ) {
				output = SQLService.Factory.getInstance().exportSQLFileFromDatabase(id);
			}
			else if ("csv".equalsIgnoreCase(exportType ) ) {
				output = CSVService.Factory.getInstance().exportDatabase(physicalDatabase.getDatabaseServer(),
						physicalDatabase.getDbConsumedName(), false);
			}
			else if ("zip".equalsIgnoreCase(exportType ) ) {
				output = CSVService.Factory.getInstance().exportDatabase(physicalDatabase.getDatabaseServer(),
						physicalDatabase.getDbConsumedName(), true);
			}
			else {
				throw new BadParameterException ( "Unable to create file type: "+exportType);
			}
			DatabaseAuditService.Factory.getInstance().createExportRecord(id);
		}
		catch (BadParameterException ex) {
			return Response.status(Response.Status.NOT_FOUND).build();
		} 
		catch (Exception e) {
			e.printStackTrace();
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
		}
		ResponseBuilder response = Response.ok(output, "application/octet-stream");
		response.header("Content-Disposition", "attachment; filename="+output.getName());
		return response.build();
	}
	

	@ApiOperation(
		value="Handles file upload for and existing database file",
		notes="CSV, Access and sql dump files are currently supported"
	)
	@ApiResponses(value = { 
		@ApiResponse(code = 201, message = "Database successfully imported."),
		@ApiResponse(code = 415, message = "Database type not supported."),
		@ApiResponse(code = 403, message = "Not authorized to create databases.")
	})
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
			
			DatabaseAuditService.Factory.getInstance().createNotAuthRecord("POST " + dbId + "/data/" + server, dbId);

			return Response.status(Response.Status.FORBIDDEN).build();
		}
		
		//MediaType contentType = fileAttachment.getContentType();
		//contentType.
		int newDbId = 0;
		MultivaluedMap<String, String> map = fileAttachment.getHeaders();
		String fileName = getFileName(map);
		String extension = FileUtilities.getFileExtension(fileName);
		if ( extension == null || (!extension.equalsIgnoreCase("csv") &&
				!extension.equalsIgnoreCase("mdb") &&
				!extension.equalsIgnoreCase("accdb"))) {
			return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE).build();
		}
		try {
			File dbFile = this.saveFileAttachment(fileAttachment, context, fileName);
			if ( extension.equalsIgnoreCase("sql")) {
				newDbId = DatabaseUploadService.Factory.getInstance().importToExistingDatabase(dbId, dbFile, server);
				DatabaseAuditService.Factory.getInstance().createImportRecord(dbId);
			}
			else {
				newDbId = DatabaseUploadService.Factory.getInstance().createNewDatabaseFromFile(dbId, dbFile, extension, server);
				DatabaseAuditService.Factory.getInstance().createImportRecord(newDbId);
			}
		}
		catch (Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
	    UriBuilder builder = uriInfo.getAbsolutePathBuilder();
	    builder.path(Integer.toString(newDbId));
	    return Response.created(builder.build()).build();
	}
	
	
	// ------------------------------------------------------------------------------
	// SQL and CSV file import
	// ------------------------------------------------------------------------------
	@ApiOperation(
			value="Handles file import to an existing database",
			notes="CSV and sql dump files are currently supported"
		)
		@ApiResponses(value = { 
			@ApiResponse(code = 201, message = "Database successfully imported."),
			@ApiResponse(code = 415, message = "Database type not supported."),
			@ApiResponse(code = 403, message = "Not authorized to create databases.")
		})
	@POST
	@Path("{id}/import/{server}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response importFile(@PathParam("id") int dbId,
			@PathParam("server") String server,
			@Multipart("databaseFile") Attachment fileAttachment,
			@Context ServletContext context, @Context UriInfo uriInfo) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService()
					.getRecordFromId(dbId);

			if (!SecurityUtils.getSubject()
					.isPermitted(DatabasePermissions.DATABASE_MODIFY(
							physicalDatabase.getLogicalDatabaseId()))) {

				DatabaseAuditService.Factory.getInstance()
						.createNotAuthRecord("POST " + dbId + "/import/", dbId);

				return Response.status(Response.Status.FORBIDDEN).build();
			}
			MultivaluedMap<String, String> map = fileAttachment.getHeaders();
			String fileName = getFileName(map);
			String extension = FileUtilities.getFileExtension(fileName);
			if (!extension.equalsIgnoreCase("sql")
					&& !extension.contentEquals("csv")) {
				return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE)
						.build();
			}
			File importFile = this.saveFileAttachment(fileAttachment, context,
					fileName);
			int newDbId;
			if (extension.equalsIgnoreCase("sql")) {
				newDbId = DatabaseUploadService.Factory.getInstance()
						.importToExistingDatabase(dbId, importFile, server);
				DatabaseAuditService.Factory.getInstance()
						.createImportRecord(dbId);
			} else {
				newDbId = DatabaseUploadService.Factory.getInstance()
						.createNewDatabaseFromFile(dbId, importFile, extension,
								server);
				DatabaseAuditService.Factory.getInstance()
						.createImportRecord(newDbId);
			}
			UriBuilder builder = uriInfo.getAbsolutePathBuilder();
			builder.path(Integer.toString(newDbId));
			return Response.created(builder.build()).build();

		} catch (BadParameterException ex) {
			return Response.status(Response.Status.NOT_FOUND).build();
		} catch (Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.build();
		}
	}

	
	@ApiOperation(
			value="Checks progress of import",
			notes="CSV and sql dump files are currently supported"
		)
		@ApiResponses(value = { 
			@ApiResponse(code = 200, message = "Call successful."),
			@ApiResponse(code = 403, message = "Not authorized to view database.")
		})

	@GET
	@Path("{id}/import")
	@Produces(MediaType.APPLICATION_JSON)
	public Response importProgress( @PathParam("id") int dbId) {
		try {
			OrdsPhysicalDatabase physicalDatabase = databaseRecordService().getRecordFromId(dbId);
			if ( !SecurityUtils.getSubject().isPermitted(DatabasePermissions.DATABASE_VIEW(physicalDatabase.getLogicalDatabaseId()))) {
				DatabaseAuditService.Factory.getInstance().createNotAuthRecord("VIEW " + dbId + "/import", dbId);
				
				return Response.status(Response.Status.FORBIDDEN).build();
			}
			OrdsPhysicalDatabase db = DatabaseRecordService.Factory.getInstance().getRecordFromId(dbId);
			ImportProgress progress = new ImportProgress();
			progress.setStatus(db.getImportProgress().toString());
			return Response.ok(progress).build();
		}
		catch (Exception e ) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e).build();
		}
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
				
				DatabaseAuditService.Factory.getInstance().createNotAuthRecord("DELETE " + dbId + "/test/delete/" + staging, dbId);

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
			@SuppressWarnings("unused")
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

    private File saveFileAttachment ( Attachment fileAttachment, ServletContext context, String fileName  ) throws Exception {
		File tempDirectory = FileUtilities.createTemporaryDirectory();
		File dbFile = new File(tempDirectory, fileName);
		
		DataHandler handler = fileAttachment.getDataHandler();
		InputStream stream = handler.getInputStream();
		FileUtilities.saveFile(dbFile, stream);
		return dbFile;
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
