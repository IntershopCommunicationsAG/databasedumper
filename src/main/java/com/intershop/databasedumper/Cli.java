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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command line interface implementation.
 */
class Cli {

	private static final Logger LOG = LoggerFactory.getLogger(Cli.class);

	private static final String JDBC_URL_PROPERTY = "intershop.jdbc.url";
	private static final String JDBC_USER_NAME_PROPERTY = "intershop.jdbc.user";
	private static final String JDBC_USER_PASSWORD_PROPERTY = "intershop.jdbc.password";

	private static final String USAGE = "[-e | -i] -f <file> -p <file> [-b <file>] [-r <number>]";
	private static final String HEADER = "databaseDumper - This programm is for creating backups of database table content and restore "
			+ "it back into a database with the same table-schema. The connection information for the database will be read from a "
			+ "properties file. Copyright 2017 Intershop Communications AG.";
	private static final String FOOTER = "For more instructions, see: https://github.com/IntershopCommunicationsAG/databaseDumper";

	private final Options options = new Options();

	private boolean exportConf = false;
	private boolean importConf = false;

	private boolean forceImport = false;
	
	private File contentFile = null;

	private List<String> blackListedTables = Collections.emptyList();

	private String jdbcUrl = "";
	private String jdbcUser = "";
	private String jdbcPassword = "";

	private int rowLimit = 0;

	/**
	 * Constructor for command line helper
	 */
	public Cli(String[] args) throws Exception {
		OptionGroup expOrImp = new OptionGroup();

		expOrImp.addOption(Option.builder("e").longOpt("export").hasArg(false)
				.desc("Export mode to write database data to a file.").build());

		expOrImp.addOption(Option.builder("i").longOpt("import").hasArg(false)
				.desc("Import mode to write data file to database. NOTE: Affected tables are cleared before the import!")
				.build());

		expOrImp.addOption(
				Option.builder("h").longOpt("help").hasArg(false).desc("Show usage of databaseDumper").build());

		expOrImp.setRequired(true);

		options.addOptionGroup(expOrImp);

		options.addOption(Option.builder("f").longOpt("file").hasArg(true).required(true)
				.desc("Path to the dump-file. File is loaded tp database with import. File will be created with export. Export fails if file already exists.")
				.build());

		options.addOption(Option.builder("p").longOpt("properties").hasArg(true).required(true)
				.desc("Path to the properties file with database connection configuration.").build());

		options.addOption(Option.builder("b").longOpt("blacklist").hasArg(true)
				.desc("File with a list of tables that will be ignored.").build());

		options.addOption(Option.builder("r").longOpt("rowlimit").hasArg(true)
				.desc("Maximum number of rows that will be processed as a batch.").build());

		options.addOption(Option.builder("t").longOpt("testImport").hasArg(false).required(false)
				.desc("Test the import run while the validation fails.").build());
		
		parse(args);
	}

	public boolean runExport() {
		return exportConf;
	}

	public boolean runImport() {
		return importConf;
	}

	public boolean runForceImport() {
		return forceImport;
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

			if (cmd.hasOption("t"))
			{
				forceImport = true;
			}
			
			if (cmd.hasOption("f")) {
				contentFile = new File(cmd.getOptionValue("f"));

				if (cmd.hasOption("e") && contentFile.exists()) {
					throw new Exception("The target file exists! Please change the file or remove the file ("
							+ contentFile.getAbsolutePath() + ")");
				}
				if (cmd.hasOption("i") && !contentFile.exists()) {
					throw new Exception("The source file (" + contentFile.getAbsolutePath() + ") does not exists!");
				}
			} else {
				throw new Exception("It is necessary to specify the target file");
			}

			if (cmd.hasOption("p")) {
				File propertiesFile = new File(cmd.getOptionValue("p"));
				if (!propertiesFile.exists()) {
					throw new Exception("The properties file with database configuration does not exists. Check "
							+ propertiesFile.getAbsolutePath());
				}
				Properties props = getPropertiesFromFile(propertiesFile);
				jdbcUrl = props.getProperty(JDBC_URL_PROPERTY, "");
				if (jdbcUrl == null || jdbcUrl.isEmpty()) {
					throw new Exception("JDBC url is not included in properties '" + propertiesFile.getAbsolutePath()
							+ "'. Check file for '" + JDBC_URL_PROPERTY + "'.");
				}
				jdbcUser = props.getProperty(JDBC_USER_NAME_PROPERTY, "");
				if (jdbcUser == null || jdbcUser.isEmpty()) {
					LOG.warn("JDBC user is not configured in properties '{}'. Check file for '{}'.",
							propertiesFile.getAbsolutePath(), JDBC_USER_NAME_PROPERTY);
				}
				jdbcPassword = props.getProperty(JDBC_USER_PASSWORD_PROPERTY, "");
				if (jdbcUser != null && !jdbcUser.isEmpty() && (jdbcPassword == null || jdbcPassword.isEmpty())) {
					throw new Exception(
							"JDBC password is not configured in properties '" + propertiesFile.getAbsolutePath()
									+ "'. Check file for '" + JDBC_USER_PASSWORD_PROPERTY + "'.");
				}
			} else {
				throw new Exception("It is necessary to specify a properties file with database connection parameter.");
			}

			if (cmd.hasOption("b")) {
				File blackListFile = new File(cmd.getOptionValue("b"));
				if (!blackListFile.exists()) {
					LOG.warn("The blacklist file + '{}' does not exists!", blackListFile.getAbsolutePath());
				} else {
					blackListedTables = getBlackListedTablesFromFile(blackListFile);
				}
			}

			if (cmd.hasOption("r")) {
				String rowLimitStr = cmd.getOptionValue("r");
				try {
					// parse the command param argument
					int rowLimitParam = Integer.parseInt(rowLimitStr);
					// check if row limit param is positive
					if (rowLimitParam >= 0) {
						// yes, set the parameter value
						rowLimit = rowLimitParam;
					} else {
						// no, invalid parameter used
						LOG.warn("The rowlimit parameter must be a positive number, but it is '{}'. Using default value '1000'.", rowLimitParam);
					}
				} catch (NumberFormatException nfe) {
					throw new Exception("The rowlimit parameter must be a positive number, but it is '" + rowLimitStr + "'.");
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to parse comand line properties: {}", e.getMessage());
			help();
		}
	}

	private void help() {
		HelpFormatter helpFormatter = new HelpFormatter();
		helpFormatter.setWidth(100);
		helpFormatter.printHelp(USAGE, HEADER, options, FOOTER);
		System.exit(2);
	}

	private List<String> getBlackListedTablesFromFile(File blackListFile) {
		List<String> list = new ArrayList<>();

		try {
			list = Files.readAllLines(Paths.get(blackListFile.toURI())).stream().filter(Objects::nonNull).filter(line -> line.length() > 0)
					.map(line -> line.trim().toUpperCase()).collect(Collectors.toList());
		} catch (IOException iox) {
			LOG.error("Failed to read the blacklist file '{}'.", blackListFile, iox);
		}

		return list;
	}

	private  Properties getPropertiesFromFile(File propertiesFile) throws Exception {

		Properties props = new Properties();
		try {
			FileInputStream in = new FileInputStream(propertiesFile);
			props.load(in);
		} catch (IOException iox) {
			throw new Exception("It was not possible to read the properties file '" + propertiesFile.getAbsolutePath()
					+ "' (" + iox.getMessage() + ")");
		}

		return props;
	}
}
