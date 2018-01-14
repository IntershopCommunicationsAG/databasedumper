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
package com.intershop.databasedumper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Information and queries for supported databases.
 * The name is returned from the available database connection.
 */
public enum SupportedDatabase {

    ORACLE ("Oracle",
            "select constraint_name from user_constraints where constraint_type = 'R' and table_name = ?",
            "alter table %s disable constraint %s",
            "alter table %s enable constraint %s"),
    MSSQL ("Microsoft SQL Server",
            "select CONSTRAINT_NAME from information_schema.table_constraints where constraint_type = 'FOREIGN KEY' and TABLE_NAME = ?",
            "alter table %s nocheck constraint %s",
            "alter table %s check constraint %s"),
    H2 ("H2",
            "",
            "alter table %s set REFERENTIAL_INTEGRITY FALSE",
            "alter table %s set REFERENTIAL_INTEGRITY TRUE");

    private final String productName;
    private final String constraintQuery;
    private final String disableConstraintStatement;
    private final String enableConstraintStatement;

    private static final Map<String, SupportedDatabase> strDatabaseMap;

    SupportedDatabase(String productName, String constraintQuery, String disableConstraintStatement, String enableConstraintStatement) {
        this.productName = productName;
        this.constraintQuery = constraintQuery;
        this.disableConstraintStatement = disableConstraintStatement;
        this.enableConstraintStatement = enableConstraintStatement;
    }

    public String getConstraintQuery() {
        return constraintQuery;
    }

    public String getDisableConstraintStatement() {
        return disableConstraintStatement;
    }

    public String getEnableConstraintStatement() {
        return enableConstraintStatement;
    }

    static {
        final Map<String, SupportedDatabase> tmpMap = Maps.newHashMap();
        for(final SupportedDatabase en : SupportedDatabase.values()) {
            tmpMap.put(en.productName, en);
        }
        strDatabaseMap = ImmutableMap.copyOf(tmpMap);
    }

    public static SupportedDatabase getSupportedDatabase(String productName) {
        if (!strDatabaseMap.containsKey(productName)) {
            throw new IllegalArgumentException("Unknown supported database " + productName);
        }
        return strDatabaseMap.get(productName);
    }

    @Override
    public String toString() {
        return productName;
    }
}
