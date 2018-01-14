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
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.sql.SQLException
import java.util.zip.ZipEntry
import java.util.zip.ZipFile

@Slf4j
@CompileStatic
class SpecDatbaseUtil {

    static List<String> getZipContent(File zipFile) {
        List<String> zipContent = []
        ZipFile zf = new ZipFile(zipFile)
        zf.entries().each { ZipEntry entry ->
            zipContent.add(entry.name)
        }
        return zipContent
    }

    static prepareEmptySchema(Sql sql) {
        try {
            int accessRows = sql.rows('select APP_ID from ACCESSDATA').size()
            if(accessRows > 0) {
                sql.execute("DROP table ACCESSDATA")
            }
        } catch (SQLException ex) {
            log.info('Table ACCESSDATA can not be dropped.')
            log.debug('SQL Exception thrown {}', ex.getMessage())
        }

        try {
            int dbUserRows = sql.rows('select USER_ID from DBUSER').size()
            if(dbUserRows > 0) {
                sql.execute("DROP table DBUSER")
            }
        } catch (SQLException ex) {
            log.info('Table DBUSER can not be dropped.')
            log.debug('SQL Exception thrown {}', ex.getMessage())
        }
    }

    static prepareDBUSER(Sql sql) {
        (1..4).each {
            sql.execute("INSERT INTO DBUSER(USER_ID, USERNAME, CREATED_BY) VALUES (${it}, 'Testuser${it}', 'system')".toString())
        }
    }

    static prepareACCESSDATA(Sql sql) {
        (1..4).each {
            sql.execute("INSERT INTO ACCESSDATA(APP_ID, USER_ID, APPNAME, ROLENAME, CREATED_BY) " +
                    "VALUES (${it}, 1, 'AppName${it}', 'RoleName${it}', 'system')".toString())
        }
        sql.execute("INSERT INTO ACCESSDATA(APP_ID, USER_ID, APPNAME, ROLENAME, CREATED_BY) " +
                "VALUES (5, 2, 'AppName1', 'RoleName1', 'system')")
        sql.execute("INSERT INTO ACCESSDATA(APP_ID, USER_ID, APPNAME, ROLENAME, CREATED_BY) " +
                "VALUES (6, 2, 'AppName2', 'RoleName2', 'system')")
        sql.execute("INSERT INTO ACCESSDATA(APP_ID, USER_ID, APPNAME, ROLENAME, CREATED_BY) " +
                "VALUES (7, 3, 'AppName3', 'RoleName3', 'system')")
        sql.execute("INSERT INTO ACCESSDATA(APP_ID, USER_ID, APPNAME, ROLENAME, CREATED_BY) " +
                "VALUES (8, 4, 'AppName4', 'RoleName4', 'system')")
    }
}
