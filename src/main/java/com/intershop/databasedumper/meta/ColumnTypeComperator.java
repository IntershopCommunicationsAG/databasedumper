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
 * Used to compare columntypes.
 */
public class ColumnTypeComperator
{
    private final List<Set<Integer>> list = new ArrayList<>();

    public ColumnTypeComperator()
    {
        super();
        initMap();
    }

    private void initMap()
    {
        put(Types.VARCHAR, Types.NVARCHAR);
        put(Types.NUMERIC, Types.FLOAT, Types.DOUBLE, Types.DECIMAL);
        put(Types.TIMESTAMP, Types.OTHER);
        put(Types.BLOB, Types.VARBINARY);
        put(Types.CLOB, Types.NVARCHAR, Types.LONGNVARCHAR);
    }

    private void put(Integer... values)
    {
        Set<Integer> set = Arrays.stream(values).collect(Collectors.toSet());
        list.add(set);
    }

    public boolean matches(Integer sourceType, Integer type)
    {
        for (Set<Integer> set : list)
        {
            if (set.contains(sourceType) && set.contains(type))
            {
                return true;
            }
        }
        return false;
    }
}
