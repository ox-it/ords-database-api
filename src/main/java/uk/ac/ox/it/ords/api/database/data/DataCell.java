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

import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * A convenience class holding information about a specific cell within a table
 * 
 * @author dave
 *
 */
public class DataCell implements Serializable {
    private Logger log = LoggerFactory.getLogger(DataCell.class);
	public enum DataType implements Serializable { 
        ARRAY,
        BIGINT,
        BINARY,
        BIT,
        BLOB,
        BOOLEAN,
        CHAR,
        CLOB,
        DATALINK,
        DATE,
        DECIMAL,
        DISTINCT,
        DOUBLE,
        FLOAT,
        INTEGER,
        JAVA_OBJECT,
        LONGNVARCHAR,
        LONGVARBINARY,
        LONGVARCHAR,
        NCHAR,
        NCLOB,
        NULL,
        NUMERIC,
        NVARCHAR,
        OTHER,
        REAL,
        REF,
        ROWID,
        SMALLINT,
        SQLXML,
        STRUCT,
        TIME,
        TIMESTAMP,
        TINYINT,
        VARBINARY,
        VARCHAR
    };
	private String value;
	private DataType type;
	private String defaultValue;
	
	
	public DataCell() {
		type = DataType.VARCHAR;
		value = null;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
        /*
         * Here is possibly where I need to manipulate the string - It might be better to do this
         * in the getter because then I would have unadulterated data in the object, but doing the
         * work every time in the getter would use more CPU. Thus here for now.
         * I need to make sure that special characters are escaped so that once they are displayed
         * in an input box they are displayed correctly. 
         * E.g. o'leary should become o\'leary,
         * o'leary is displayed as o
         * o&apos;'leary is displayed as o'leary
         */
        if (value == null) {
            this.value = null;
        }
        else {
            this.value = value.replaceAll("'", "&apos;");
            if (log.isDebugEnabled()) {
                if (value.contains("'")) {
                    log.debug(String.format("Value contains single quote - replacing <%s> with <%s>", value, this.value));
                }
            }
        }
	}

	public DataType getType() {
		return type;
	}

	public void setType(DataType type) {
		this.type = type;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
}
