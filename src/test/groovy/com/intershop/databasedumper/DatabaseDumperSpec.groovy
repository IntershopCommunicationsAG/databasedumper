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

import com.intershop.gradle.test.util.TestDir
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import spock.lang.Specification

@Slf4j
abstract class DatabaseDumperSpec extends Specification {

    @TestDir
    File testDir

    abstract void prepareDatabase()

    abstract String getJDBCURL()

    abstract String getJDBCUser()

    abstract String getJDBCPassword()

    abstract Sql getPreparedSql()

    def 'check prepared database'() {
        given:
        prepareDatabase()
        Sql sql = getPreparedSql()

        when:
        List<GroovyRowResult> rowDBUserResultList = sql.rows('select USER_ID, USERNAME, CREATED_BY from DBUSER where USER_ID=1')
        List<GroovyRowResult> rowResultList = sql.rows('select u.USER_ID, u.USERNAME, a.APPNAME from DBUSER u, ACCESSDATA a where u.USER_ID=a.USER_ID and u.USER_ID=1')

        then:
        rowDBUserResultList.size() == 1
        rowDBUserResultList.get(0).get('USERNAME') == 'Testuser1'

        rowResultList.size() == 4
        rowResultList.get(0).get('APPNAME') == 'AppName1'

        cleanup:
        sql.close()
    }

    def 'Test dumper import and export'() {
        given:
        prepareDatabase()
        testDir.mkdir()
        File testContent = new File(testDir, 'testfile.zip')

        when: 'Export with database dumper'
        DatabaseDumper ddExport = new DatabaseDumper(getJDBCURL(), getJDBCUser(), getJDBCPassword(), testContent, 0, Collections.<String>emptyList())

        then:
        ddExport.runExport()
        testContent.exists()
        List<String> list = SpecDatbaseUtil.getZipContent(testContent)
        list.contains('ACCESSDATA.xml')
        list.contains('metadata/ACCESSDATA.xml')
        list.contains('DBUSER.xml')
        list.contains('metadata/DBUSER.xml')

        when: 'Extend the database'
        Sql sql = getPreparedSql()
        sql.execute("INSERT INTO DBUSER(USER_ID, USERNAME, CREATED_BY) VALUES (5, 'Testuser5', 'system')")
        sql.execute("INSERT INTO ACCESSDATA(APP_ID, USER_ID, APPNAME, ROLENAME, CREATED_BY) " +
                "VALUES (9, 5, 'AppName5', 'RoleName5', 'system')")
        List<GroovyRowResult> rowDBUserResultList = sql.rows('select USER_ID, USERNAME, CREATED_BY from DBUSER where USER_ID=5')
        List<GroovyRowResult> rowResultList = sql.rows('select u.USER_ID, u.USERNAME, a.APPNAME from DBUSER u, ACCESSDATA a where u.USER_ID=a.USER_ID and u.USER_ID=5')

        then:
        rowDBUserResultList.size() == 1
        rowDBUserResultList.get(0).get('USERNAME') == 'Testuser5'

        rowResultList.size() == 1
        rowResultList.get(0).get('APPNAME') == 'AppName5'

        when: 'Import with database dumper'
        DatabaseDumper ddImport = new DatabaseDumper(getJDBCURL(), getJDBCUser(), getJDBCPassword(), testContent, 0, Collections.<String>emptyList())

        then:
        ddImport.runImport()
        List<GroovyRowResult> rowDBUserResultListAfterImport = sql.rows('select USER_ID, USERNAME, CREATED_BY from DBUSER where USER_ID=5')
        List<GroovyRowResult> rowResultListAfterImport = sql.rows('select u.USER_ID, u.USERNAME, a.APPNAME from DBUSER u, ACCESSDATA a where u.USER_ID=a.USER_ID and u.USER_ID=5')
        rowDBUserResultListAfterImport.size() == 0
        rowResultListAfterImport.size() == 0

        cleanup:
        sql.close()
    }


}
