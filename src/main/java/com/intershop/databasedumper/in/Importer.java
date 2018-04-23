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
package com.intershop.databasedumper.in;

import com.intershop.databasedumper.SupportedDatabase;
import com.intershop.databasedumper.meta.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Class with main import functionality: verification and import.
 */
public class Importer
{
    private static final Logger LOG = LoggerFactory.getLogger(Importer.class);

    private ImportHandler importHandler;
    private Map<String, Set<String>> constraints = new ConcurrentHashMap<>();
    private final File contentFile;
    private final String username;
    private final ConnectionFactory conFactory;

    private SupportedDatabase database;

    public Importer(final String url, final String username, final String password, final File file,
                    final int maxRows) throws Exception
    {
        super();
        System.setProperty("jdk.xml.totalEntitySizeLimit", "" + Integer.MAX_VALUE);

        this.contentFile = file;
        this.username = username.toUpperCase();
        conFactory = new ConnectionFactory(url, username, password);

        try {
            database = SupportedDatabase.getSupportedDatabase(getDatabaseType());
        } catch (Exception ex) {
			throw new Exception("It was not possible to identify the database (" + getDatabaseType() + ")!");
        }

        importHandler = new ImportHandler(conFactory, database, file, maxRows);
    }

    /**
     * Get the name of the database from connection meta data.
     * @return the name of the used database product
     * @throws SQLException
     */
    private String getDatabaseType() throws SQLException {
    	// create a new database connection
        try (Connection conn = conFactory.create())
        {
	        // return the name of the database
	        return conn.getMetaData().getDatabaseProductName();
        }
    }

    public void doImport(boolean forceImport) throws JAXBException, IOException, SQLException,
                    ParserConfigurationException, SAXException
    {
        LOG.info("Beginning the validation!");
        boolean valid = checkTableMetadata();
        if (!valid && !forceImport)
        {
            LOG.error("The destination-database does not seems to fit the given data. Import aborted!");
            return;
        }
        else if (!valid && forceImport)
        {
        	LOG.warn("Continue import because 'testImport' option is set while validation failed!");
        }
        else
        {
        	LOG.info("Finished the validation! Validation was successfully: {}", valid);
        }
        
        // disable constraints
        preProcessing();
        // import all data
        importHandler.importData();
        // enable constraints
        postProcessing();
    }

    public Set<String> readTableNames(final ZipEntryFilter filter) throws IOException
    {
        Set<String> result = new TreeSet<>();
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(contentFile)))
        {
            ZipEntry entry;
            while((entry = in.getNextEntry()) != null)
            {
                if (filter.accept(entry))
                {
                    result.add(entry.getName());
                }
                in.closeEntry();
            }
        }
        return result;
    }

    private void preProcessing() throws IOException, SQLException
    {
        LOG.info("Starting the pre-processing!");

        Set<String> tableNames = importHandler.getZipper().getTableNames();
        LOG.info("Removing constraints.");
        try (final Connection con = conFactory.create())
        {
        	for (String s : tableNames)
        	{
                importHandler.readConstraints(con, s, constraints);
            }
        }
        ExecutorService disableConstraintsService = Executors.newWorkStealingPool();
		for (Map.Entry<String, Set<String>> entry : constraints.entrySet()) {
			final String tablename = entry.getKey();
			if (!entry.getValue().isEmpty()) {
				for (String constraint : entry.getValue()) {
					disableConstraintsService.execute(() -> {
						try (Connection con = conFactory.create()) {
							importHandler.disableConstraint(con, tablename, constraint);
						} catch (SQLException e) {
							LOG.error(e.getLocalizedMessage(), e);
						}
					});
				}
			} else if (database.getConstraintQuery().isEmpty()) {
				try (Connection con = conFactory.create()) {
					importHandler.disableConstraints(con, tablename);
				} catch (SQLException e) {
					LOG.error(e.getLocalizedMessage(), e);
				}
			}
		}
        disableConstraintsService.shutdown();
        try
        {
            boolean allDisabled = disableConstraintsService.awaitTermination(2, TimeUnit.HOURS);
            if (!allDisabled)
            {
                LOG.error("Could not disable all constraints within 2 hours.");
            }
        }
        catch(InterruptedException e)
        {
            LOG.error(e.getLocalizedMessage(), e);
        }

        LOG.info("Deleting content.");
        ExecutorService deleteService = Executors.newWorkStealingPool(4);
        for (String s : tableNames)
        {
            deleteService.execute(() -> {
                try (Connection con = conFactory.create())
                {
                    importHandler.deleteTableContent(con, s);
                }
                catch(SQLException e)
                {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            });
        }
        deleteService.shutdown();
        try
        {
            boolean allDisabled = deleteService.awaitTermination(2, TimeUnit.HOURS);
            if (!allDisabled)
            {
                LOG.error("Could not disable all constraints within 2 hours.");
            }
        }
        catch(InterruptedException e)
        {
            LOG.error(e.getLocalizedMessage(), e);
        }
        
        LOG.info("Finished the pre-processing!");
    }

    private void postProcessing()
    {
        LOG.info("Beginning the post-processing!");
        ExecutorService enableConstraintsService = Executors.newWorkStealingPool(4);
        for (Map.Entry<String, Set<String>> entry : constraints.entrySet())
        {
            final String tableName = entry.getKey();
            if(! entry.getValue().isEmpty()) {
                for (final String s : entry.getValue()) {
                    enableConstraintsService.execute(() -> {
                        try (Connection con = conFactory.create()) {
                            importHandler.enableConstraint(con, tableName, s);
                        } catch (SQLException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                        }
                    });
                }
            } else if(database.getConstraintQuery().isEmpty()) {
                try (Connection con = conFactory.create()) {
                    importHandler.enableConstraints(con, tableName);
                } catch (SQLException e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            }
        }
        enableConstraintsService.shutdown();
        try
        {
            boolean allEnabled = enableConstraintsService.awaitTermination(2, TimeUnit.HOURS);
            if (!allEnabled)
            {
                LOG.error("Could not enable all of the constraints!");
            }
        }
        catch(InterruptedException e)
        {
            LOG.error(e.getLocalizedMessage(), e);
        }
        
        LOG.info("Finished the post-processing!");
    }

    private boolean checkTableMetadata() throws JAXBException, IOException, SQLException
    {
        boolean result = true;
        ValidationResult validationResult = new ValidationResult();
        try (Connection con = conFactory.create())
        {
            importHandler.setScheme(username, con);
        }
        ExecutorService validateService = Executors.newWorkStealingPool();
        List<Table> tables = importHandler.getZipper().getTables();
        try
        {
            for (final Table table : tables)
            {
                validateService.execute(() -> {
                    try (Connection con = conFactory.create())
                    {
                        importHandler.validate(table, con);
                    }
                    catch(Exception e)
                    {
                    	validationResult.addValidationError();
                        LOG.error(e.getLocalizedMessage(), e);
                        throw new IllegalStateException(e);
                    }
                });
            }

        }
        catch(IllegalStateException e)
        {
            result = false;
            LOG.error(e.getLocalizedMessage(), e);
        }
        validateService.shutdown();
        try
        {
            boolean success = validateService.awaitTermination(2, TimeUnit.HOURS);
            if (!success)
            {
                result = false;
                LOG.error("Could not finish the validation within 2 hours.");
            }
        }
        catch(InterruptedException e)
        {
            LOG.error(e.getLocalizedMessage(), e);
            result = false;
        }

        return result & validationResult.getValidationErrors() == 0;
//        return result;
    }
    
    class ValidationResult {
    	int validationErrors = 0;

		public int getValidationErrors() {
			return validationErrors;
		}

		public void addValidationError() {
			LOG.info("Add validation error...");
			this.validationErrors++;
		}
    	
    }
}
