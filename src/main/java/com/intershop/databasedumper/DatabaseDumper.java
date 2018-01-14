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

import com.intershop.databasedumper.ex.ExportHandler;
import com.intershop.databasedumper.in.Importer;
import com.intershop.databasedumper.meta.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.zip.ZipOutputStream;

/**
 * DatabaseDumper can
 * <ul>
 * <li>export the content of a database into a compressed file or</li>
 * <li>extract the files content into another database</li>
 * </ul>
 */
public class DatabaseDumper
{
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseDumper.class);
    
    public static final String META_DIR_NAME = "metadata";

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    private final File contentFile;

    private final int rowLimit;
    private final List<String> blacklist;

    /**
     * Main method to run the command line tool of DatabaseDumper.
     *
     * @param args These are all command line parameters.
     * @throws Exception An exception is thrown if something is not correct configured in the command line.
     */
    public static void main(String[] args) throws Exception {
        Cli cli = new Cli(args);

        DatabaseDumper dumper = new DatabaseDumper(cli.getJdbcUrl(),
                                   cli.getJdbcUser(),
                                   cli.getJdbcPassword(),
                                   cli.getContentFile(),
                                   cli.getRowLimit(),
                                   cli.getBlackListedTables());

        if(cli.runImport()) {
            if(dumper.runImport()) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        } else if(cli.runExport()) {
            if(dumper.runExport()) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        }
    }

    /**
     * Constructor of DatabaseDumper
     *
     * @param jdbcUrl       JDBC url for database connection
     * @param jdbcUser      User for databse access (optional)
     * @param jdbcPassword  Password for database access (optional)
     * @param contentFile   Content file for export or import
     * @param rowLimit      Row limit for export
     * @param blacklist     Black listed tables for export
     */
    public DatabaseDumper(String jdbcUrl, String jdbcUser, String jdbcPassword, File contentFile, int rowLimit, List<String> blacklist) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
        this.contentFile = contentFile;
        this.rowLimit = rowLimit;
        this.blacklist = blacklist;
    }

    /**
     * Import method
     *
     * @return  true, if the process was successful
     */
    public boolean runImport() {
        try {
            Importer importer = new Importer(jdbcUrl, jdbcUser, jdbcPassword, contentFile, this.rowLimit);
            importer.doImport();
            return true;
        } catch(Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
        }

        return false;
    }

    /**
     * Export method
     *
     * @return  true, if the process was successful
     */
    public boolean runExport() {
        try {
            createExportPackage();
            return true;
        } catch (JAXBException | IOException | SQLException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }

        return false;
    }

    /**
     * Run export and zip the content
     *
     * @throws JAXBException
     * @throws IOException
     * @throws SQLException
     */
    @SuppressWarnings("JavaDoc")
    private void createExportPackage() throws JAXBException, IOException, SQLException {
        ExportHandler handler = new ExportHandler();        
        if (rowLimit > 0) {
            handler.setMaxRows(rowLimit);
        }

        LOG.info("Destination for export is : {}", contentFile.getAbsolutePath());

        try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(contentFile))) {
            out.setLevel(9);
            handler.getZipper().setOut(out);

            List<Table> tables;

            Connection con;

            if(jdbcUser != null && ! jdbcPassword.isEmpty()) {
                con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
                tables = handler.readTableNames(con, jdbcUser.toUpperCase());
            } else {
                con = DriverManager.getConnection(jdbcUrl);
                tables = handler.readTableNames(con, null);
            }

            for (Table table : tables) {
                if (containsCaseInsensitive(table.getName(), blacklist)) {
                    LOG.info("Ignoring table {}, because it is part of the blacklist!", table.getName());
                    continue;
                }

                LOG.info("Reading table {}", table.getName());
                handler.readData(table, con);
                LOG.info("Wrote entry for {}", table.getName());
            }
        }
    }

    private boolean containsCaseInsensitive(String s, List<String> l){
        return l.stream().anyMatch(x -> x.equalsIgnoreCase(s));
    }
}
