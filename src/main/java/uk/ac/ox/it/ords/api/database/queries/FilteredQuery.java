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

package uk.ac.ox.it.ords.api.database.queries;

import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;
import uk.ac.ox.it.ords.api.database.data.DataTypeMap;

public class FilteredQuery {
	
	String filter;
	ParameterList parameters;
	
	public FilteredQuery(String filter, String params, boolean isCaseSensitive){
		try {
			this.parameters = getParameterList(params);
		} catch (Exception e) {
			this.parameters = new ParameterList();
		}
		this.filter = createFilterFromParameters(normaliseFilterQuery(filter), parameters, isCaseSensitive);
		if (this.parameters == null){
			this.parameters = new ParameterList();
		}
	}
	
	public FilteredQuery(String filter, ParameterList params, boolean isCaseSensitive){
		this.parameters = params;
		this.filter = createFilterFromParameters(normaliseFilterQuery(filter), parameters, isCaseSensitive);
		if (this.parameters == null){
			this.parameters = new ParameterList();
		}
	}
	
	public FilteredQuery(){
		
	}
	
	/**
	 * Returns a query based on a filter string and parameter string
	 */
	public String getFilter( 
			String filter, 
			String params, 
			boolean isCaseSensitive
			) throws ClassNotFoundException, SQLException, Exception {
		ParameterList parameters = getParameterList(params);
		String query;
		try {
			query = createFilterFromParameters(normaliseFilterQuery(filter), parameters, isCaseSensitive);
		} catch (Exception e) {
			return null; // invalid query
		}
		return query;
	}
	
	/**
	 * Normalises input from RQB by replacing any aliases such as "x0" with
	 * actual table names
	 * 
	 * @param filter
	 * @return the normalised filter
	 */
	protected String normaliseFilterQuery(String filter){
		
		if (filter == null) return null;
		
		//
		// We need to replace any temporary identifiers from RedQueryBuilder with the actual referenced table names
		//
		// RQB gives us filters that look like this:
		//
		// SELECT * FROM "documents" INNER JOIN "sites" "x0" ON "docsite" = "x0"."id" WHERE ("x0"."sitename" = ?)"
		//
		// We need to make this instead:
		//
		// SELECT * FROM "documents" INNER JOIN "sites" ON "docsite" = "sites"."id" WHERE ("sites"."sitename" = ?)"
		//
		// Ideally we should be able to specify within RQB not to use these aliases at all ...
		//
		// Note also here the 20 is an arbitrary number - if tables have more than 20 foreign key relations
		// with filter conditions attached then this is going to fail.
		//
		for (int i = 0; i < 20; i++){
			String identifier = String.format("\"x%d\"", i);
			//
			// Potentially we could get problems with legitimate uses of "x0" in queries
			// so we check for '"x0" ON' which should be uniquely used by these cases
			//
			if (filter.contains(identifier+" ON ")){
				//
				// Find the table identifier used before the identifier
				// first take the part form before the first use of the identifier
				// e.g. SELECT * FROM "documents" INNER JOIN "sites" 
				//
				String part = filter.split(identifier + " ON ")[0].trim();
				//
				// Now get the last word
				//
				String name = part.substring(part.lastIndexOf(" ")+1);
				//
				// ... and replace all occurences where the identifier is used
				//
				filter = filter.replaceAll(identifier, name);
			} else {
				//
				// Another case is a simpler filter such as:
				//
				// SELECT "x0".* FROM "city" "x0" WHERE ("x0"."population" > ?)
				//
				// For this we get the table identifier from just before the WHERE clause
				//
				if (filter.contains(identifier + " WHERE")){
					String part = filter.split(identifier + " WHERE")[0].trim();
					//
					// Now get the last word
					//
					String name = part.substring(part.lastIndexOf(" ")+1);
					//
					// ... and replace all occurences where the identifier is used
					//
					filter = filter.replaceAll(identifier, name);
				}
			}
			
		}
		return filter;
	}
	
	/**
	 * Converts a parameter string into a List of parameters
	 * @param params
	 * @return
	 * @throws Exception 
	 */
	protected ParameterList getParameterList(String params) throws Exception{

		ParameterList parameterList = new ParameterList();

		if (params != null && !params.trim().isEmpty()){

			//
			// remove any HTML quotes and replace with single quotes
			//
			params = params.replaceAll("&quot;", "'");

			try {
				JSONArray parameterArray = new JSONArray(params);
				for (int i = 0; i < parameterArray.length(); i++){
					
					JSONObject parameterObject = parameterArray.getJSONObject(i);
					String type = parameterObject.getString("type");

					if (type.equals("string")){
						String value = parameterObject.getString("value");
						parameterList.addParameter(value);
					}
					if (type.equals("boolean")){
						boolean value = parameterObject.getBoolean("value");
						parameterList.addParameter(value);
					}
					if (type.equals("number")){
						try {
							int value = parameterObject.getInt("value");
							parameterList.addParameter(value);
						} catch (JSONException j){
							parameterList.addNull();							
						}
						
					}
					if (type.equals("date")){
						DataTypeMap map = new DataTypeMap();
						map.dt = DataType.TIMESTAMP;
						String value = parameterObject.getString("value");
						value = value.replace("T", " ");
						value = value.replace("Z", "");
						map.stringValue = value;
						parameterList.addParameter(map);
					}
				}
			}
			catch (JSONException e) {
				return null;
			}
		}
		return parameterList;
	}

	/**
	 * Construct a single SQL query from a parameterised request, as supplied
	 * from the filter interface via table.jsp
	 * 
	 * @param filter the filter SQL string
	 * @param params the array of parameters
	 * @return a combined SQL query, or null if either of the components are invalid
	 */
	protected String createFilterFromParameters(String filter, ParameterList parameters, boolean isCaseSensitive){
		//
		// We don't want malformed queries...
		//
		if (filter == null || parameters == null) return null;
		if (filter.equals("null") || filter.trim().isEmpty()) return null;

		//
		// Do the number of parameters match the number of placeholders?
		//
		int count = StringUtils.countMatches(filter, "?");
		if (count > 0 && count != parameters.size()){
			return null;
		}

		//
		// Substitute 'CONTAINS param' with 'LIKE %param%'
		//
		if (filter.contains(" CONTAINS ")){
			//
			// Identify the parameter that applies
			//
			while (filter.contains(" CONTAINS ")){
			int index = StringUtils.countMatches(filter.split(" CONTAINS ")[0], "?");
			filter = filter.replaceFirst(" CONTAINS ", " LIKE ");
			String parameter = (String)parameters.getParameter(index).stringValue;
			//
			// Replace surrounding single quotes with '%
			//
			parameters.setParameterValue(index,"%"+parameter+"%");
			}
		}
		
		//
		// Substitute 'LIKE' with 'ILIKE" or the reverse, as appropriate
		//
		if (isCaseSensitive){
			filter = filter.replace(" ILIKE ", " LIKE ");
		} else {
			filter = filter.replace(" LIKE ", " ILIKE ");
		}

		//
		// replace HTML quotes in SQL with double quotes
		//
		filter = filter.replaceAll("&quot;", "\"");

		return filter;
	}



	public String getFilter() {
		return filter;
	}



	public ParameterList getParameters() {
		return parameters;
	}
}
