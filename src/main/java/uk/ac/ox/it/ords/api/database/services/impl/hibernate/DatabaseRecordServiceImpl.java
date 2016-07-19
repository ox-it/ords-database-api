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
import java.util.ArrayList;

import org.apache.shiro.SecurityUtils;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.SimpleExpression;

import uk.ac.ox.it.ords.api.database.model.OrdsDB;
import uk.ac.ox.it.ords.api.database.model.OrdsPhysicalDatabase;
import uk.ac.ox.it.ords.api.database.services.DatabaseRecordService;
import uk.ac.ox.it.ords.security.permissions.Permissions;

public class DatabaseRecordServiceImpl extends DatabaseServiceImpl implements DatabaseRecordService {

	@Override
	public OrdsPhysicalDatabase getRecordFromGivenName(String givenName)
			throws Exception {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("dbName", givenName));
		OrdsDB odb = this.getModelObject(exprs, OrdsDB.class, false);
		
		exprs.clear();
		exprs.add(Restrictions.eq("logicalDatabaseId", odb.getLogicalDatabaseId()));
		
		return this.getModelObject(exprs, OrdsPhysicalDatabase.class, true);
	}

	@Override
	public OrdsPhysicalDatabase getRecordFromId(int id) {
		ArrayList<SimpleExpression> exprs = new ArrayList<SimpleExpression>();
		exprs.add(Restrictions.eq("physicalDatabaseId", id));
		return this.getModelObject(exprs, OrdsPhysicalDatabase.class, true);
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public List<OrdsPhysicalDatabase> getDatabaseList() throws Exception {
		
		List<OrdsPhysicalDatabase> databases = null;
		ArrayList<OrdsPhysicalDatabase> visibleDatabases = new ArrayList<OrdsPhysicalDatabase>();
		
		Session session = this.getOrdsDBSessionFactory().openSession();
		try {
			Transaction transaction = session.beginTransaction();
			databases = session
					.createCriteria(OrdsPhysicalDatabase.class)
					.list();
			transaction.commit();
		} catch (Exception e) {
			log.debug(e.getMessage());
			session.getTransaction().rollback();
			throw e;
		}
		finally {
			session.close();
		}

		for (OrdsPhysicalDatabase database : databases){
			if (SecurityUtils.getSubject().isPermitted(Permissions.DATABASE_VIEW(database.getLogicalDatabaseId()))){
				visibleDatabases.add(database);
			}
		}
		
		return visibleDatabases;
	}	

	

}
