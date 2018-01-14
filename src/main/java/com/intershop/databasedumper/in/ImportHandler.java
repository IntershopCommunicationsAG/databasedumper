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
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.parsers.ParserConfigurationException;

import com.intershop.databasedumper.SupportedDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Column;
import com.intershop.databasedumper.meta.ColumnTypeComperator;
import com.intershop.databasedumper.meta.Row;
import com.intershop.databasedumper.meta.Table;

class ImportHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ImportHandler.class);

    private static final int MAX_ROWS = 1000;

    private int maxRows = MAX_ROWS;

    private String scheme;

    private ZipReader zipper;

    private final SupportedDatabase database;

    public ImportHandler(SupportedDatabase database) {
        this.database = database;
    }

    public void writeData(final ConnectionFactory conFactory)
                    throws IOException, SQLException, ParserConfigurationException, SAXException
    {
        zipper.forEachDataTable(this, conFactory);
    }

    public void writeData(final DataTable dataTable, final Connection con) throws SQLException
    {
        LOG.info("Writing into {} ", dataTable.getTable().getName());
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
                            stm.setBigDecimal(index, get(obj));
                            break;
                        case Types.VARCHAR:
                        case Types.NVARCHAR:
                            stm.setString(index, get(obj));
                            break;
                        case Types.TIMESTAMP:
                        case Types.OTHER:
                            Timestamp timestamp = null;
                            XMLGregorianCalendar cal = get(obj);
                            if (cal != null)
                            {
                                timestamp = new Timestamp(cal.toGregorianCalendar().getTimeInMillis());
                            }
                            stm.setTimestamp(index, timestamp);
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
                        default:
                            throw new IllegalStateException(
                                            "Unsupported type at table " + dataTable.getTable().getName() + '.'
                                                            + column.getLabel() + " : " + column.getType());

                    }
                }
                stm.addBatch();
                if (rowCount >= maxRows)
                {
                    LOG.info("Beginning part-commit for table {}", dataTable.getTable().getName());
                    stm.executeBatch();
                    LOG.info("... partly ...");
                    rowCount = 0;
                }
                else
                {
                    ++rowCount;
                }
            }
            stm.executeBatch();
            LOG.info(" done.");
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
    
    public void disableConstraint(final Connection con, final String tablename, final String constraint) throws SQLException {
        try (PreparedStatement stm = con
                        .prepareStatement(String.format(database.getDisableConstraintStatement(), tablename, constraint)))
        {
            LOG.info("alter table {} disable constraint {}", tablename, constraint);
            stm.executeUpdate();
        }
    }

    public void disableConstraints(final Connection con, final String tablename) throws SQLException {
        try (PreparedStatement stm = con
                .prepareStatement(String.format(database.getDisableConstraintStatement(), tablename)))
        {
            LOG.info("alter table {} disable constraint", tablename);
            stm.executeUpdate();
        }
    }
    
    public void enableConstraint(final  Connection con, final String tableName, final String constraint) throws SQLException {
        try (PreparedStatement stm = con
                        .prepareStatement(String.format(database.getEnableConstraintStatement(), tableName, constraint)))
        {
            LOG.info("alter table {} enable constraint {}", tableName, constraint);
            stm.executeUpdate();
        }
    }

    public void enableConstraints(final  Connection con, final String tableName) throws SQLException {
        try (PreparedStatement stm = con
                .prepareStatement(String.format(database.getEnableConstraintStatement(), tableName)))
        {
            LOG.info("alter table {} enable constraint", tableName);
            stm.executeUpdate();
        }
    }

    public void deleteTableContent(final Connection con, final String tableName)
    {
        try (PreparedStatement stm = con.prepareStatement(String.format("delete from %s", tableName)))
        {
            int executed = stm.executeUpdate();
            LOG.info("Deleted entries from table {}: {}", tableName, executed);
        }
        catch(SQLException e)
        {
            throw new IllegalStateException("Could not delete content of table " + tableName, e);
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
        ColumnTypeComperator comperator = new ColumnTypeComperator();

        boolean tableExists;

        tableExists = validateTable(table,con, table.getName(), this.scheme);
        if(! tableExists) {
            tableExists = validateTable(table,con, table.getName(),null);
        }
        if (! tableExists) {
            tableExists = validateTable(table ,con, table.getName().toLowerCase(), null);
        }

        if (!tableExists)
        {
            throw new IllegalStateException(
                    "Table " + table.getName() + " does not exists in the target database!");
        }
    }

    private boolean validateTable(final Table table, final Connection con, String tableName, String scheme) throws SQLException
    {
        ColumnTypeComperator comperator = new ColumnTypeComperator();
        boolean tableExists = false;

        try (ResultSet resultSet = con.getMetaData().getColumns(null, this.scheme, tableName, null)) {
            final Map<String, Integer> columnMap = new TreeMap<>();
            table.getColumns().forEach(c -> columnMap.put(c.getLabel(), c.getType()));
            while (resultSet.next()) {
                tableExists = true;
                String name = resultSet.getString("COLUMN_NAME");
                int type = resultSet.getInt("DATA_TYPE");
                Integer sourceType = columnMap.remove(name);
                if (sourceType == null) {
                    LOG.warn("The column {} of the table {} does not exists in the source!", name, tableName);
                } else if (!comperator.matches(sourceType.intValue(), Integer.valueOf(type))) {
                    throw new IllegalStateException("In the table " + table.getName() + ", the column " + name
                            + " has the type " + type + ", but expected was something that could be handled as "
                            + sourceType);
                }
            }

            if (tableExists && !columnMap.isEmpty()) {
                throw new IllegalStateException("In the destination-database, the table " + tableName
                        + " is missing at least one column! Missing: "
                        + columnMap.keySet().stream().collect(Collectors.joining(", ")));
            }
        }

        return tableExists;
    }

    public ZipReader getZipper()
    {
        if (zipper == null)
        {
            try
            {
                zipper = new ZipReader();
            }
            catch(JAXBException e)
            {
                throw new IllegalStateException("Could not create ZipReader!", e);
            }
        }
        return zipper;
    }

    public void setMaxRows(int maxRows)
    {
        this.maxRows = maxRows == 0 ? MAX_ROWS : maxRows;
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
