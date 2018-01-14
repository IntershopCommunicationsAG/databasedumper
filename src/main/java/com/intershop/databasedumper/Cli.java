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

import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Command line interface implementation.
 */
class Cli {

    private static final Logger log = Logger.getLogger(Cli.class.getName());

    private static final String JDBC_URL_PROPERTY = "intershop.jdbc.url";
    private static final String JDBC_USER_NAME_PROPERTY = "intershop.jdbc.user";
    private static final String JDBC_USER_PASSWORD_PROPERTY = "intershop.jdbc.password";

    private static final String USAGE = "[-e | -i] -f <file> -p <file> [-b <file>] [-r <number>]";
    private static final String HEADER = "databaseDumper - This programm is for creating backups of database-table-content and plays " +
            "it back into the database or another one with the same table-schema. The connections for the database will be read from a " +
            "propertie file. Copyright 2017 Intershop Communications AG.";
    private static final String FOOTER = "For more instructions, see: https://github.com/IntershopCommunicationsAG/databaseDumper";

    private final Options options = new Options();

    private boolean exportConf = false;
    private boolean importConf = false;

    private File contentFile = null;

    private List<String> blackListedTables = Collections.emptyList();

    private String jdbcUrl = "";
    private String jdbcUser = "";
    private String jdbcPassword = "";

    private int rowLimit = 0;

    /**
     * Construcutor for command line helper
     */
    public Cli(String[] args) throws Exception {
        OptionGroup expOrImp = new OptionGroup();

        expOrImp.addOption(Option.builder("e")
                .longOpt("export")
                .hasArg(false)
                .desc("call export mode of database dumper")
                .build());

        expOrImp.addOption(Option.builder("i")
                .longOpt("import")
                .hasArg(false)
                .desc("call import mode of database dumper")
                .build());

        expOrImp.setRequired(true);

        options.addOptionGroup(expOrImp);

        options.addOption(Option.builder("f")
                .longOpt("file")
                .hasArg(true)
                .required(true)
                .desc("path to the dump-file, will be created if it not yet exists")
                .build());

        options.addOption(Option.builder("p")
                .longOpt("properties")
                .hasArg(true)
                .required(true)
                .desc("path to the properties for database configuration")
                .build());

        options.addOption(Option.builder("b")
                .longOpt("blacklist")
                .hasArg(true)
                .desc("file with a list of tables that will be ignored")
                .build());

        options.addOption(Option.builder("r")
                .longOpt("rowlimit")
                .hasArg(true)
                .desc("maximum count of rows that will be read in the cache before a write attemp will be done.")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .hasArg(false)
                .desc("Show usage of databaseDumper")
                .build());

        parse(args);
    }

    public boolean runExport() {
        return exportConf;
    }

    public boolean runImport() {
        return importConf;
    }

    public File getContentFile() {
        return contentFile;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getJdbcUser() {
        return jdbcUser;
    }

    public String getJdbcPassword() {
        return jdbcPassword;
    }

    public List<String> getBlackListedTables() {
        return blackListedTables;
    }

    public int getRowLimit() {
        return rowLimit;
    }

    private void parse(String[] args) throws Exception {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h")) {
                help();
            }

            if (cmd.hasOption("e")) {
                exportConf = true;
            } else if (cmd.hasOption("i")) {
                importConf = true;
            }

            if (cmd.hasOption("f")) {
                contentFile = new File(cmd.getOptionValue("f"));

                if(cmd.hasOption("e") && contentFile.exists()) {
                    throw new Exception("The target file exists! Please change the file or remove the file (" + contentFile.getAbsolutePath() + ")");
                }
                if(cmd.hasOption("i") && ! contentFile.exists()) {
                    throw new Exception("The source file (" + contentFile.getAbsolutePath() + ") does not exists!");
                }
            } else {
                throw new Exception("It is necessary to specify the target file");
            }

            if (cmd.hasOption("p")) {
                File propertiesFile = new File(cmd.getOptionValue("p"));
                if(! propertiesFile.exists()) {
                    throw new Exception("The properties file with database configuration does not exists. Check " + propertiesFile.getAbsolutePath());
                }
                Properties props = getPropertiesFromFile(propertiesFile);
                jdbcUrl = props.getProperty(JDBC_URL_PROPERTY, "");
                if(jdbcUrl == null || jdbcUrl.isEmpty()) {
                    throw new Exception("JDBC url is not included in properties '" + propertiesFile.getAbsolutePath() + "'. Check file for '" + JDBC_URL_PROPERTY + "'.");
                }
                jdbcUser = props.getProperty(JDBC_USER_NAME_PROPERTY, "");
                if(jdbcUser == null || jdbcUser.isEmpty()) {
                    log.warning("JDBC user is not configured in properties '" + propertiesFile.getAbsolutePath() + "'. Check file for '" + JDBC_USER_NAME_PROPERTY + "'.");
                }
                jdbcPassword = props.getProperty(JDBC_USER_PASSWORD_PROPERTY, "");
                if(jdbcUser != null && ! jdbcUser.isEmpty() && (jdbcPassword == null || jdbcPassword.isEmpty())) {
                    throw new Exception("JDBC password is not configured in properties '" + propertiesFile.getAbsolutePath() + "'. Check file for '" + JDBC_USER_PASSWORD_PROPERTY + "'.");
                }
            } else {
                throw new Exception("It is necessary to specify a properties file with database connection parameter.");
            }

            if (cmd.hasOption("b")) {
                File blackListFile = new File(cmd.getOptionValue("b"));
                if(! blackListFile.exists()) {
                    log.warning("The blacklist file + '" + blackListFile.getAbsolutePath() + "' does not exists!");
                } else {
                    blackListedTables = getBlackListedTablesFromFile(blackListFile);
                }
            }

            if (cmd.hasOption("r")) {
                String rowLimitStr = cmd.getOptionValue("r");
                try {
                    rowLimit = Integer.parseInt(rowLimitStr);
                }catch (NumberFormatException nfe) {
                    throw new Exception("The rowlimit parameter must be a number, but it is '" + rowLimitStr + "'.");
                }
            }

        } catch (ParseException e) {

            log.log(Level.SEVERE, "Failed to parse comand line properties", e);
            help();
        }
    }

    private void help() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth( 100 );
        helpFormatter.printHelp(USAGE, HEADER, options, FOOTER );
        System.exit(0);
    }

    private static List<String> getBlackListedTablesFromFile(File blackListFile){
        List<String> list = new ArrayList<>();

        try {
            list = Files.readAllLines(Paths.get(blackListFile.toURI())).stream()
                    .filter(Objects::nonNull).map(line -> line.trim().toUpperCase())
                    .collect(Collectors.toList());
        } catch(IOException iox) {
            log.warning("It was not possible to read the blacklist file '" + blackListFile + "' (" + iox.getMessage() + ")");
        }

        return list;
    }

    private static Properties getPropertiesFromFile(File propertiesFile) throws Exception {

        Properties props = new Properties();
        try {
            FileInputStream in = new FileInputStream(propertiesFile);
            props.load(in);
        } catch (IOException iox) {
            throw new Exception("It was not possible to read the properties file '" + propertiesFile.getAbsolutePath() + "' (" + iox.getMessage() + ")" );
        }

        return props;
    }
}

