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
package uk.ac.ox.it.ords.api.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.apache.shiro.mgt.SecurityManager;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ox.it.ords.security.AbstractShiroTest;
import uk.ac.ox.it.ords.security.RemoteUserToken;


public class DatabasePermissionTest extends AbstractShiroTest{

	@BeforeClass
	public static void setup() {
		Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:shiro.ini");
		SecurityManager securityManager = factory.getInstance();
		SecurityUtils.setSecurityManager(securityManager);
	}

	@Test
	public void checkPermission(){
		Subject subjectUnderTest = new Subject.Builder(getSecurityManager()).buildSubject();
		setSubject(subjectUnderTest);

		AuthenticationToken token = new RemoteUserToken("scott.wilson@shibboleth.ox.ac.uk", "member@ox.ac.uk");
		subjectUnderTest.login(token);	

		assertEquals("scott.wilson@shibboleth.ox.ac.uk",SecurityUtils.getSubject().getPrincipal().toString());
		assertTrue(SecurityUtils.getSecurityManager().hasRole(SecurityUtils.getSubject().getPrincipals(), "owner_2"));

		// Is owner
		assertTrue(SecurityUtils.getSubject().isPermitted("database:modify:2"));
		assertTrue(SecurityUtils.getSubject().isPermitted("project:delete:2"));

		// Is not owner
		assertFalse(SecurityUtils.getSubject().isPermitted("database:modify:3"));
		assertFalse(SecurityUtils.getSubject().isPermitted("project:delete:3"));

		//
		//  user with view only permissions
		//
		token = new RemoteUserToken("pingu", "");
		subjectUnderTest.login(token);	
		assertFalse(SecurityUtils.getSubject().isPermitted("database:modify:1"));
		assertTrue(SecurityUtils.getSubject().isPermitted("database:view:1"));
		assertTrue(SecurityUtils.getSubject().isPermitted("database:view:2"));
		assertFalse(SecurityUtils.getSubject().isPermitted("database:delete:2"));
	}

	@Test (expected=AuthenticationException.class)
	public void nullRemoteUser() {
		Subject subjectUnderTest = new Subject.Builder(getSecurityManager()).buildSubject();
		setSubject(subjectUnderTest);
		AuthenticationToken token = new RemoteUserToken(null, null);
		subjectUnderTest.login(token);	
	}

	@Test (expected=AuthenticationException.class)
	public void emptyRemoteUser() {
		Subject subjectUnderTest = new Subject.Builder(getSecurityManager()).buildSubject();
		setSubject(subjectUnderTest);
		AuthenticationToken token = new RemoteUserToken("", "");
		subjectUnderTest.login(token);	
	}


}
