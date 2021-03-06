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

package uk.ac.ox.it.ords.api.database.data;

import uk.ac.ox.it.ords.api.database.model.TableView;

public class TableViewInfo {
	private int	id;
	private String viewName;
	private String viewTable;
	private String viewDescription;
	private String viewQuery;
	private String viewAuthorization;
	private int databaseId;

	public TableViewInfo(){
		
	}
	
	public TableViewInfo(TableView dataset){
		this.setId(dataset.getId());
		this.setViewAuthorization(dataset.getTvAuthorization());
		this.setViewDescription(dataset.getViewDescription());
		this.setViewName(dataset.getViewName());
		this.setViewQuery(dataset.getQuery());
		this.setViewTable(dataset.getAssociatedTable());
		this.setDatabaseId(dataset.getPhysicalDatabaseId());
	}
	
	public String getViewName() {
		return viewName;
	}
	public void setViewName(String viewName) {
		this.viewName = viewName;
	}
	public String getViewDescription() {
		return viewDescription;
	}
	public void setViewDescription(String viewDescription) {
		this.viewDescription = viewDescription;
	}
	public String getViewQuery() {
		return viewQuery;
	}
	public void setViewQuery(String viewQuery) {
		this.viewQuery = viewQuery;
	}
	public String getViewAuthorization() {
		return viewAuthorization;
	}
	public void setViewAuthorization(String viewAuthorization) {
		this.viewAuthorization = viewAuthorization;
	}
	public String getViewTable() {
		return viewTable;
	}
	public void setViewTable(String viewTable) {
		this.viewTable = viewTable;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(int databaseId) {
		this.databaseId = databaseId;
	}



}