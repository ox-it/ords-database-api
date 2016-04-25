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
package uk.ac.ox.it.ords.api.database.documentation;

import org.apache.cxf.jaxrs.swagger.Swagger2Feature;

/**
 * 
 * Swagger documentation configuration.
 * This is handled in a subclass as its not currently possible to configure Swagger2Feature without
 * using Spring.
 *
 */
public class Swagger extends Swagger2Feature {
	
	public Swagger(){
		super();
		this.setBasePath("/api/1.0/database/");
		this.setDescription("API for managing the contents of ORDS databases");
		this.setTitle("ORDS Database Query API");
		this.setVersion("1.0");
		this.setContact("ords@it.ox.ac.uk");
	}

}
