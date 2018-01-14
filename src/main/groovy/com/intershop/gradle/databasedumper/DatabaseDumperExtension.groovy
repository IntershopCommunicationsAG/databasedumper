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

import com.intershop.gradle.databasedumper.connection.Connection
import groovy.transform.CompileStatic
import org.gradle.api.Project

/**
 *   This is the complete extension of this plugin.
 *
 *   databaseDumper {
 *      connection {
 *          url
 *          username
 *          password
 *      }
 *
 *      contentFile
 *
 *      rowlimit
 *
 *      tableBlacklist = []
 *   }
 *
 **/
@CompileStatic
class DatabaseDumperExtension {

    private Project project

    DatabaseDumperExtension(Project project) {
        this.project = project

        this.connection = new Connection()
        this.connection.username = ''
        this.connection.password = ''

        this.rowLimit = 0
        this.tableBlacklist = []
    }

    /**
     * This represents the necessary
     * database connection.
     */
    Connection connection

    void connection(Closure c) {
        project.configure(connection, c)
    }

    /**
     * This file is used for import and export.
     */
    File contentFile

    /**
     * This is the maximal row number processing in import
     * and export. If this number is not configured, the
     * maximal values for import or export are used.
     */
    int rowLimit

    /**
     * This list of tables will be not processed.
     * This configuration is used only for export.
     */
    List<String> tableBlacklist

}
