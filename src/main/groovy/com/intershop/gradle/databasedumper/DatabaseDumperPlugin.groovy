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

import groovy.transform.CompileStatic
import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * This plugin will apply the databaseDumperPlugin.
 *
 * <p>It adds a task for export DB data to a Zip file.
 * With an import task it is possible to write the data
 * of the Zip file to an existing data schema.</p>
 *
 * <p>Before the import of a table runs all data in
 * the original table will be deleted.</p>
 */
@CompileStatic
class DatabaseDumperPlugin implements Plugin<Project> {

    /**
     * Name of the extension
     */
    static final String DBDUMPER_EXTENSION = 'databaseDumper'

    /**
     * Task names and descriptions
     */
    static final String DBIMPORT_TASK = 'dbImport'
    static final String DBIMPORT_DESCR = 'Runs an import from a package file to the specified database connection.'
    static final String DBEXPORT_TASK = 'dbExport'
    static final String DBEXPORT_DESCR = 'Runs an export to a package file from the specified database connection.'

    /**
     * Task configuration
     */
    static final String TASK_GROUP = "databaseDumper Tasks"

    /**
     * Creates the extension and tasks of this plugin.
     *
     * @param project
     */
    @TypeChecked(TypeCheckingMode.SKIP)
    @Override
    void apply(Project project) {

        // Create extension
        DatabaseDumperExtension extension = project.extensions.findByType(DatabaseDumperExtension) ?: project.extensions.create(DBDUMPER_EXTENSION, DatabaseDumperExtension, project)

        DBDumperImportTask dbImportTask = project.tasks.maybeCreate(DBIMPORT_TASK, DBDumperImportTask)
        dbImportTask.group = TASK_GROUP
        dbImportTask.description = DBIMPORT_DESCR

        dbImportTask.conventionMapping.jdbcUrl = { extension.getConnection().getUrl() }
        dbImportTask.conventionMapping.jdbcUsername = { extension.getConnection().getUsername() }
        dbImportTask.conventionMapping.jdbcPassword = { extension.getConnection().getPassword() }

        dbImportTask.conventionMapping.rowLimit = { extension.getRowLimit() }
        dbImportTask.conventionMapping.blackListedTables = { extension.getTableBlacklist() }
        dbImportTask.conventionMapping.contentFile = { extension.getContentFile() }


        DBDumperExportTask dbExportTask = project.tasks.maybeCreate(DBEXPORT_TASK, DBDumperExportTask)
        dbExportTask.group = TASK_GROUP
        dbExportTask.description = DBEXPORT_DESCR

        dbExportTask.conventionMapping.jdbcUrl = { extension.getConnection().getUrl() }
        dbExportTask.conventionMapping.jdbcUsername = { extension.getConnection().getUsername() }
        dbExportTask.conventionMapping.jdbcPassword = { extension.getConnection().getPassword() }

        dbExportTask.conventionMapping.rowLimit = { extension.getRowLimit() }
        dbExportTask.conventionMapping.blackListedTables = { extension.getTableBlacklist() }
        dbExportTask.conventionMapping.contentFile = { extension.getContentFile() }
    }
}
