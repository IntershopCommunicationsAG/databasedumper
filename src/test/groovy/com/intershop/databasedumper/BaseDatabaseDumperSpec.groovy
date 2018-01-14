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
package com.intershop.databasedumper

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import org.junit.Rule
import org.zapodot.junit.db.EmbeddedDatabaseRule

import java.sql.SQLException

@Slf4j
class BaseDatabaseDumperSpec extends DatabaseDumperSpec {

    @Rule
    EmbeddedDatabaseRule dbRule = EmbeddedDatabaseRule
            .builder()
            .withMode("ORACLE").build()


    void prepareDatabase() throws SQLException {
        Sql sql = getPreparedSql()

        SpecDatbaseUtil.prepareEmptySchema(sql)

        sql.execute('CREATE TABLE DBUSER(USER_ID NUMBER(5) NOT NULL, USERNAME VARCHAR(20) NOT NULL, ' +
                'CREATED_BY VARCHAR(20) NOT NULL, PRIMARY KEY (USER_ID))')

        SpecDatbaseUtil.prepareDBUSER(sql)

        sql.execute('CREATE TABLE ACCESSDATA(APP_ID NUMBER(5) NOT NULL, USER_ID NUMBER(5) NOT NULL, ' +
                'APPNAME VARCHAR(20) NOT NULL, ROLENAME VARCHAR(20) NOT NULL, CREATED_BY VARCHAR(20) NOT NULL, ' +
                'PRIMARY KEY (APP_ID), FOREIGN KEY (USER_ID) REFERENCES DBUSER(USER_ID))')

        SpecDatbaseUtil.prepareACCESSDATA(sql)

        sql.close()
    }

    String getJDBCURL() {
        return dbRule.getConnectionJdbcUrl()
    }

    String getJDBCUser() {
        return ''
    }

    String getJDBCPassword() {
        return ''
    }

    Sql getPreparedSql() {
        return Sql.newInstance(getJDBCURL())
    }

}
