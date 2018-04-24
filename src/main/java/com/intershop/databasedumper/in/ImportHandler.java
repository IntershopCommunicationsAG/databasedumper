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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.intershop.databasedumper.SupportedDatabase;
import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Column;
import com.intershop.databasedumper.meta.ColumnTypeComperator;
import com.intershop.databasedumper.meta.Row;
import com.intershop.databasedumper.meta.Table;

class ImportHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ImportHandler.class);

    public static final int MAX_ROWS = 1000;
    private int maxRows = MAX_ROWS;

    private String scheme;

    private ZipReader zipper;

    private final SupportedDatabase database;

    private ConnectionFactory connectionFactory = null;
    
    public ImportHandler(ConnectionFactory conFactory, SupportedDatabase database, File importFile, int maxRows) {
        this.connectionFactory = conFactory;
    	this.database = database;
        
        try
        {
            zipper = new ZipReader(importFile);
        }
        catch(JAXBException e)
        {
            throw new IllegalStateException("Could not create ZipReader!", e);
        }
        
        this.maxRows = maxRows <= 0 ? MAX_ROWS : maxRows;
    }

	public void importData()
			throws IOException, SQLException, ParserConfigurationException, SAXException {
        LOG.info("Beginning with the main-process!");
		zipper.importDataTables(this, connectionFactory);
        LOG.info("Finished the main-process!");
	}

    public void writeImportData(final DataTable dataTable, final Connection con) throws SQLException
    {
        LOG.info("Writing data into {} ", dataTable.getTable().getName());
        StringBuilder insertStr = new StringBuilder("insert into " + dataTable.getTable().getName() + "(");
        String collect = dataTable.getTable().getColumns().stream().map(Column::getLabel)
                        .collect(Collectors.joining(","));
        insertStr.append(collect).append(')');
        insertStr.append("values(");
        String questionMarks = dataTable.getTable().getColumns().stream().map(s -> "?")
                        .collect(Collectors.joining(","));
        insertStr.append(questionMarks);
        insertStr.append(")");

        try (PreparedStatement stm = con.prepareStatement(insertStr.toString()))
        {
            TimeZone tz = TimeZone.getTimeZone("UTC");
            Calendar cal = Calendar.getInstance(tz);

            int rowCount = 0;
            for (Row row : dataTable.getRows())
            {
                int index = 0;
                for (Column column : dataTable.getTable().getColumns())
                {
                    Object obj = row.getElement().get(index);
                    ++index;
                    switch(column.getType())
                    {
                        case Types.NUMERIC:
                        case Types.DOUBLE:
                        case Types.DECIMAL:
                        case Types.FLOAT:
                        case Types.BIT:
                        case Types.BIGINT:
                        case Types.SMALLINT:
                        case Types.INTEGER:
                        case Types.TINYINT:
                            stm.setBigDecimal(index, get(obj));
                            break;
                        case Types.VARCHAR:
                        case Types.NVARCHAR:
                            stm.setString(index, get(obj));
                            break;
                        case Types.TIMESTAMP:
                        case Types.TIME:
                            Timestamp timestamp = null;
                            XMLGregorianCalendar xmlCal = get(obj);
                            if (xmlCal != null)
                            {
                                timestamp = new Timestamp(xmlCal.toGregorianCalendar().getTimeInMillis());
                            }
                            stm.setTimestamp(index, timestamp, cal);
                            break;
                        case Types.BLOB:
                        case Types.VARBINARY:
                            byte[] blob = get(obj);
                            stm.setBlob(index, new ByteArrayInputStream(blob));
                            break;
                        case Types.CLOB:
                        case Types.LONGNVARCHAR:
                            String clobStr = get(obj);
                            Reader reader = null;
                            if (clobStr != null && clobStr.length() > 0)
                            {
                                reader = new StringReader(clobStr);
                            }
                            stm.setClob(index, reader);
                            break;
                        case Types.OTHER:
                        default:
                            throw new IllegalStateException(
                                            "Unsupported type at table " + dataTable.getTable().getName() + '.'
                                                            + column.getLabel() + " : " + java.sql.JDBCType.valueOf(column.getType()).getName());

                    }
                }
                stm.addBatch();
                if (rowCount >= maxRows)
                {
                    LOG.info("Commit {} rows batch of table {}", rowCount, dataTable.getTable().getName());
                    stm.executeBatch();
                    LOG.info("... continue ...");
                    rowCount = 0;
                }
                else
                {
                    ++rowCount;
                }
            }
            stm.executeBatch();
            LOG.info("Finished import of table {} with {} rows batch commit.", dataTable.getTable().getName(), rowCount);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T get(Object obj)
    {
        return (T)obj;
    }

    public void readConstraints(final Connection con, final String tablename, final Map<String, Set<String>> constraints) throws SQLException {
        Set<String> cons = new LinkedHashSet<>();
        if(database.getConstraintQuery() != null && ! database.getConstraintQuery().isEmpty()) {
            try (PreparedStatement stm = con.prepareStatement(database.getConstraintQuery())) {
                stm.setString(1, tablename);
                try (ResultSet resSet = stm.executeQuery()) {
                    while (resSet.next()) {
                        cons.add(resSet.getString(1));
                    }
                }
            }
            constraints.put(tablename, cons);
        } else {
            constraints.put(tablename, new HashSet<>());
        }
    }
    
    public void disableConstraint(final Connection con, final String tableName, final String constraint) throws SQLException {
        String disableConstraintSQL = String.format(database.getDisableConstraintStatement(), tableName, constraint);
		try (PreparedStatement stm = con
                        .prepareStatement(disableConstraintSQL))
        {
            LOG.info("Disable contraint {} on {} with '{}'", constraint, tableName, disableConstraintSQL);
            stm.executeUpdate();
        }
    }

    public void disableConstraints(final Connection con, final String tableName) throws SQLException {
        String disableConstraintSQL = String.format(database.getDisableConstraintStatement(), tableName);
		try (PreparedStatement stm = con
                .prepareStatement(disableConstraintSQL))
        {
            LOG.info("Disable all contraints on {} with '{}'", tableName, disableConstraintSQL);
            stm.executeUpdate();
        }
    }
    
    public void enableConstraint(final  Connection con, final String tableName, final String constraint) throws SQLException {
        String enableContraintSQL = String.format(database.getEnableConstraintStatement(), tableName, constraint);
		try (PreparedStatement stm = con
                        .prepareStatement(enableContraintSQL))
        {
            LOG.info("Enable contraint {} on {} with '{}'", constraint, tableName, enableContraintSQL);
            stm.executeUpdate();
        }
    }

    public void enableConstraints(final  Connection con, final String tableName) throws SQLException {
        String enableContraintSQL = String.format(database.getEnableConstraintStatement(), tableName);
		try (PreparedStatement stm = con
                .prepareStatement(enableContraintSQL))
        {
            LOG.info("Enable all contraints on {} with '{}'", tableName, enableContraintSQL);
            stm.executeUpdate();
        }
    }

    public void deleteTableContent(final Connection con, final String tableName)
    {
        try (PreparedStatement stm = con.prepareStatement(String.format("delete from %s", tableName)))
        {
            int executed = stm.executeUpdate();
            LOG.info("Deleted entries of table {}: {}", tableName, executed);
        }
        catch(SQLException e)
        {
            throw new IllegalStateException("Could not delete entries of table " + tableName, e);
        }
    }

    public void validateTables(final Connection con) throws SQLException, IOException, JAXBException
    {
        for (Table table : zipper.getTables())
        {
            validate(table, con);
        }
    }

    public void validate(final Table table, final Connection con) throws SQLException
    {
        LOG.info("Validating table {}", table.getName());

        boolean tableExists = validateTable(table, con, table.getName(), this.scheme);
		if (!tableExists) {
			tableExists = validateTable(table, con, table.getName(), null);
        }
		if (!tableExists) {
			tableExists = validateTable(table, con, table.getName().toLowerCase(), null);
        }

        if (!tableExists)
        {
            throw new IllegalStateException(
                    "Table " + table.getName() + " does not exists in the target database!");
        }
    }

    private boolean validateTable(final Table sourceTable, final Connection con, String tableName, String scheme) throws SQLException
    {
        ColumnTypeComperator comperator = new ColumnTypeComperator();
        boolean tableExists = false;

        final Map<String, Integer> columnMap = new TreeMap<>();
        sourceTable.getColumns().forEach(c -> columnMap.put(c.getLabel(), c.getType()));

        // get all meta information from database
        try (ResultSet resultSet = con.getMetaData().getColumns(null, this.scheme, tableName, null)) {
            while (resultSet.next()) {
            	// column meta data is found; table exists
                tableExists = true;
                
                // get column name
                String columnName = resultSet.getString("COLUMN_NAME");
                // and type
                int columnType = resultSet.getInt("DATA_TYPE");
                
                // get type from source table
                Integer sourceType = columnMap.remove(columnName);
                
                // check if type exists in source
				if (sourceType == null) {
					// no; found column does not exists in source data
					LOG.warn("The column {} of the table {} does not exists in the source!", columnName, tableName);
				}
				//
				else if (!comperator.matches(sourceType.intValue(), Integer.valueOf(columnType))) {
					throw new IllegalArgumentException("Unexpected type: " + sourceTable.getName() + "." + columnName
							+ "(" + java.sql.JDBCType.valueOf(columnType).getName() + "). Column should be of type '"
							+ java.sql.JDBCType.valueOf(sourceType).getName() + "'.");
				}
            }

            if (tableExists && !columnMap.isEmpty()) {
				throw new IllegalStateException("The target table " + tableName + " is missing the following columns: "
						+ columnMap.keySet().stream().collect(Collectors.joining(", ")));
            }
        }

        return tableExists;
    }

    public ZipReader getZipper()
    {
        return zipper;
    }

    /**
     * Sets the scheme. 
     * Checks if the scheme is correct by selecting the tables of the collection.
     * If the check fails, the scheme is set to null.
     * @param scheme the scheme to check and use.
     * @param con the connection to the database for the check.
     * @throws SQLException  This exception is thrown if the list of tables is not readable.
     */
    public void setScheme(String scheme, final Connection con) throws SQLException
    {
        try (ResultSet tables = con.getMetaData().getTables(null, scheme, null, new String[] { "TABLE" }))
        {
            if (tables.next())
            {
                this.scheme = scheme;
            }
            else
            {
                this.scheme = null;
            }
        }
    }
}
