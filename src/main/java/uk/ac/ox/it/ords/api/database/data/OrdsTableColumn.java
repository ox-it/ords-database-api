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
import java.util.List;

import uk.ac.ox.it.ords.api.database.data.DataCell.DataType;


/**
 * A table column, in its simplest sense, is simply a column in the table. However, it could reference
 * a column in another table. Thus the raison d'etre for this class.
 * @author dave
 *
 */
public class OrdsTableColumn implements Serializable {
	public String columnName;
    public DataType columnType = DataType.OTHER;
    public String referencedColumnIndex;
	public String referencedTable = null;
	public String referencedColumn = null;
	public List<String> alternateColumns = null;
	public List<String> alternativeOptions = null;
    public int orderIndex;
    public String comment;
}
