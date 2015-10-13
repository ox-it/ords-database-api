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

package uk.ac.ox.it.ords.api.database.model;

import java.util.UUID;




public class DatabaseUploadContinuationData {
	private int id;
	private String uuid;
	private int physicalDatabaseId;
	private String schemaDir;
	private String schema;
	private String csvDir;
	
	public DatabaseUploadContinuationData() {
		setUuid(UUID.randomUUID().toString());
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public int getPhysicalDatabaseId() {
		return physicalDatabaseId;
	}

	public void setPhysicalDatabaseId(int physicalDatabaseId) {
		this.physicalDatabaseId = physicalDatabaseId;
	}

	public String getSchemaDir() {
		return schemaDir;
	}

	public void setSchemaDir(String schemaDir) {
		this.schemaDir = schemaDir;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getCsvDir() {
		return csvDir;
	}

	public void setCsvDir(String csvDir) {
		this.csvDir = csvDir;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
