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

package uk.ac.ox.it.ords.api.database.services.impl.hibernate;

import java.util.List;

import uk.ac.ox.it.ords.api.database.data.ColumnReference;
import uk.ac.ox.it.ords.api.database.data.TableData;
import uk.ac.ox.it.ords.api.database.services.QueryService;

public class QueryServiceImpl extends DatabaseServiceImpl
		implements
			QueryService {

	@Override
	public List<ColumnReference> getColumnsFromRelation(int dbId,
			String instance, String table, String foreignKeyColumn)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TableData performQuery(int dbId, String instance, String q,
			int startIndex, int rowsPerPage, String filter, String order)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
