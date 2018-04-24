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
package com.intershop.databasedumper.meta;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * Table object.
 */
@XmlRootElement
public class Table
{
    private String name;

    private List<Column> columns = new ArrayList<>();
    
    /**
     * Get the name of the table
     * @return the table name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Set the name of the table
     * @param name the table name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    public void addColumn(Column column)
    {
        columns.add(column);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        Table other = (Table)obj;
        if (columns == null)
        {
            if (other.columns != null)
                return false;
        }
        else if (!columns.equals(other.columns))
            return false;
        if (name == null)
        {
            if (other.name != null)
                return false;
        }
        else if (!name.equals(other.name))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "Table [name=" + name + ", columns=" + columns + "]";
    }

}
