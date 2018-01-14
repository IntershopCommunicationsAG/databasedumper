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
package com.intershop.gradle.databasedumper

import com.intershop.databasedumper.DatabaseDumper
import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction

@CompileStatic
class DBDumperImportTask extends AbstractDBDumperTask {

    @InputFile
    File contentFile

    @TaskAction
    void importDB() {
        // validate file
        File contentFile = getContentFile()
        if(! contentFile.exists()) {
            throw new GradleException("The import file ${contentFile.absolutePath} does not exist.")
        }

        // initialize dumper
        // initialize dumper
        DatabaseDumper dumper = new DatabaseDumper(getJdbcUrl(),
                getJdbcUsername(),
                getJdbcPassword(),
                getContentFile(),
                getRowLimit(),
                getBlackListedTables())

        // run import
        if(! dumper.runImport()) {
            throw new GradleException("Import was not successful! Please check the error log output.")
        }
    }
}
