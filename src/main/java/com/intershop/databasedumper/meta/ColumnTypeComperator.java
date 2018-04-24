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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to compare column types.
 */
public class ColumnTypeComperator
{
    // a list of equivalent type sets
    private final List<Set<Integer>> typeList = new ArrayList<>(5);

    public ColumnTypeComperator()
    {
        initMap();
    }

    private void initMap()
    {
        put(Types.VARCHAR, Types.NVARCHAR);
        put(Types.NUMERIC, Types.FLOAT, Types.DOUBLE, Types.DECIMAL, Types.BIT, Types.BIGINT, Types.SMALLINT, Types.INTEGER, Types.TINYINT);
        put(Types.TIMESTAMP, Types.OTHER);
        put(Types.BLOB, Types.VARBINARY);
        put(Types.CLOB, Types.NVARCHAR, Types.LONGNVARCHAR);
    }

    /**
     * Create a set of matching types and store to internal list.
     * @param values different database types to be equivalent
     */
    private void put(Integer... values)
    {
    	// create a set of matching types
        Set<Integer> set = Arrays.stream(values).collect(Collectors.toSet());
        // add type set to list
        typeList.add(set);
    }

    /**
     * Check if the source type and the target type are equivalent.
     * @param sourceType source column type
     * @param targetType expected target type
     * @return <code>true</code> if source type can be converted to target type and <code>false</code> otherwise
     */
    public boolean matches(Integer sourceType, Integer targetType)
    {
    	// loop all type sets
        for (Set<Integer> set : typeList)
        {
        	// check if the set contains source and target type
            if (set.contains(sourceType) && set.contains(targetType))
            {
            	// yes, match found
                return true;
            }
        }
        // no match found
        return false;
    }
    
    /**
     * Checks if the column type is known.
     * @param type the type to check
     * @return <code>true</code> if the type is known and <code>false</code> otherwise
     */
    public boolean isKnownType(Integer type)
    {
    	// loop all type sets
        for (Set<Integer> set : typeList)
        {
        	// check if the set contains source and target type
            if (set.contains(type))
            {
            	// yes, match found
                return true;
            }
        }
        // no match found
        return false;
    }
}
