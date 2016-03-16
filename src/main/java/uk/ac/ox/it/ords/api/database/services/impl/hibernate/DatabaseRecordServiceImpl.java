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

import java.util.ArrayList;

import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.SimpleExpression;

import uk.ac.ox.it.ords.api.database.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.DatabaseRecordService;

public class DatabaseRecordServiceImpl extends DatabaseServiceImpl implements DatabaseRecordService {

	@Override
	public OrdsPhysicalDatabase getRecordFromGivenName(String givenName, String instance)
			throws Exception {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("dbName", givenName));
		OrdsDB odb = this.getModelObject(exprs, OrdsDB.class);
		
		exprs.clear();
		exprs.add(Restrictions.eq("logicalDatabaseId", odb.getLogicalDatabaseId()));
		exprs.add(Restrictions.eq("entity", OrdsPhysicalDatabase.EntityType.valueOf(instance)));
		
		return this.getModelObject(exprs, OrdsPhysicalDatabase.class);
	}

}
