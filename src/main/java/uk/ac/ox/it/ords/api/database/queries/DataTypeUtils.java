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

import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;

/**
 * Utilities for mapping data types between SQL, PostgreSQL, and JDBC
 * 
 * @author scottw
 *
 */
public class DataTypeUtils {
	
    public static DataType getDataType(int ct) {
        DataType dt;
        // See java.sql.Types
        switch (ct) {
            case 2003: dt = DataType.ARRAY;
				break;
            case -5: dt = DataType.BIGINT;
				break;
            case -2: dt = DataType.BINARY;
				break;
            case -7: dt = DataType.BOOLEAN;
                // -7 is really BIT, but JDBC sees BOOLEANs as BITs and
                // we can't save BITs with PreparedStatement, so it's easiest
                // to pretend its a BOOLEAN.
				break;
            case 2004: dt = DataType.BLOB;
				break;
            case 16: dt = DataType.BOOLEAN;
				break;
            case 1: dt = DataType.CHAR;
				break;
            case 2005: dt = DataType.CLOB;
				break;
            case 70: dt = DataType.DATALINK;
				break;
            case 91: dt = DataType.DATE;
				break;
            case 3: dt = DataType.DECIMAL;
				break;
            case 2001: dt = DataType.DISTINCT;
				break;
            case 8: dt = DataType.DOUBLE;
				break;
            case 6: dt = DataType.FLOAT;
				break;
            case 4: dt = DataType.INTEGER;
				break;
            case 2000: dt = DataType.JAVA_OBJECT;
				break;
            case -16: dt = DataType.LONGNVARCHAR;
				break;
            case -4: dt = DataType.LONGVARBINARY;
				break;
            case -1: dt = DataType.LONGVARCHAR;
				break;
            case -15: dt = DataType.NCHAR;
				break;
            case 2011: dt = DataType.NCLOB;
				break;
            case 0: dt = DataType.NULL;
				break;
            case 2: dt = DataType.NUMERIC;
				break;
            case -9: dt = DataType.NVARCHAR;
				break;
            case 1111: dt = DataType.OTHER;
				break;
            case 7: dt = DataType.REAL;
				break;
            case 2006: dt = DataType.REF;
				break;
            case -8: dt = DataType.ROWID;
				break;
            case 5: dt = DataType.SMALLINT;
				break;
            case 2009: dt = DataType.SQLXML;
				break;
            case 2002: dt = DataType.STRUCT;
				break;
            case 92: dt = DataType.TIME;
				break;
            case 93: dt = DataType.TIMESTAMP;
				break;
            case -6: dt = DataType.TINYINT;
				break;
            case -3: dt = DataType.VARBINARY;
				break;
            case 12: dt = DataType.VARCHAR;
				break;
            default: dt = DataType.OTHER;
                break;
        }

        return dt;
    }
    
    
    /**
     * The following is taken from http://www.postgresql.org/docs/9.1/static/datatype.html
     * There seems to be a disconnect between what Postgres knows as a datatype and what
     * ORDS might expect (e.g. macaddr is a Postgres datatype we don't know about ... similarly
     * we have space for DataType.BLOB that doesn't appear as a Postgres datatype).
     * TODO more work needed here
     * @param type
     * @return the datatype or DataType.OTHER if unknown
     */
    public static DataType getDataType(String type) {
        if (type == null) {
        	return null;
        }
        if (type.equals("character varying")) {
        	return DataType.VARCHAR;
        }
        if (type.equals("integer")) {
        	return DataType.INTEGER;
        }
        if (type.startsWith("timestamp")) {
        	return DataType.TIMESTAMP;
        }
        if (type.equals("boolean")) {
        	return DataType.BOOLEAN;
        }
        if (type.equals("bigint")) {
        	return DataType.BIGINT;
        }
        if (type.equals("numeric")) {
        	return DataType.DECIMAL;
        }
        if (type.equals("date")) {
        	return DataType.DATE;
        }
        if (type.startsWith("double ")) {
        	return DataType.DOUBLE;
        }
        if (type.equals("xml")) {
        	return DataType.SQLXML;
        }
        if (type.equals("character")) {
        	return DataType.CHAR;
        }
        if (type.equals("time without time zone")) {
        	return DataType.TIME;
        }
        if (type.equals("time")) {
        	return DataType.TIME;
        }
        if (type.equals("timestamp")) {
        	return DataType.TIMESTAMP;
        }
        if (type.equals("smallint")) {
        	return DataType.SMALLINT;
        }
        if (type.equals("real")) {
        	return DataType.REAL;
        }
        
        return DataType.OTHER;
    }

}
