/*
 * Copyright 2017 Intershop Communications AG.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intershop.databasedumper.data;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.intershop.databasedumper.meta.Row;
import com.intershop.databasedumper.meta.Table;

/**
 * This class is used to process the storage
 * of table information.
 */
@XmlRootElement
public class DataTable implements Comparable<DataTable>
{
    private List<Row> rows = new LinkedList<>();
    private Table table;

    public Table getTable()
    {
        return table;
    }

    public void setTable(Table table)
    {
        this.table = table;
    }

    public boolean addRow(Row arg0)
    {
        return rows.add(arg0);
    }
        
    public List<Row> getRows()
    {
        return Collections.unmodifiableList(rows);
    }

    public void setRows(List<Row> rows)
    {
        this.rows = rows;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((rows == null) ? 0 : rows.hashCode());
        result = prime * result + ((table == null) ? 0 : table.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
        DataTable other = (DataTable)obj;
        if (rows == null)
        {
            if (other.rows != null)
                return false;
        }
        else if (!rows.equals(other.rows))
            return false;
        if (table == null)
        {
            if (other.table != null)
                return false;
        }
        else if (!table.equals(other.table))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "DataTable [table=" + table + "]";
    }

    @Override
    public int compareTo(DataTable o)
    {
        if(this.equals(o)) {
            return 0;
        }
        return this.getTable().getName().compareTo(o.getTable().getName());
    }

}
