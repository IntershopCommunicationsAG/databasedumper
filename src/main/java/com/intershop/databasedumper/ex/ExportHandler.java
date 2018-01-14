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
package com.intershop.databasedumper.ex;

import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Column;
import com.intershop.databasedumper.meta.Row;
import com.intershop.databasedumper.meta.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the main class for the export
 * functionlity.
 * It analyses tables and store this information
 * in separate xml data files.
 */
public class ExportHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ExportHandler.class);

    private static final int MAX_ROWS = 300000;

    private int maxRows = MAX_ROWS;
    
    private ZipWriter zipper;

    public List<Table> readTableNames(final Connection con, final String scheme) throws SQLException
    {    
        List<Table> list = new ArrayList<>();

        try (ResultSet tables = con.getMetaData().getTables(null, scheme, null, new String[] { "TABLE" }))
        {
            while(tables.next())
            {
                Table table = new Table();

                table.setName(String.valueOf(tables.getString("TABLE_NAME")).toUpperCase());

                if (table.getName().chars().filter(i -> i == '$').count() <= 1
                                && Character.isUpperCase(table.getName().charAt(0)))
                {
                    list.add(table);
                }
            }
        }

        if(list.isEmpty()) {
            list = readSimpleTableNames(con);
        }

        return list;
    }

    private List<Table> readSimpleTableNames(final Connection con) throws SQLException
    {
        List<Table> list = new ArrayList<>();

        try (ResultSet tables = con.getMetaData().getTables(null, null, null, new String[] { "TABLE" }))
        {
            while(tables.next())
            {
                Table table = new Table();

                table.setName(String.valueOf(tables.getString("TABLE_NAME")).toUpperCase());

                if (table.getName().chars().filter(i -> i == '$').count() <= 1
                        && Character.isUpperCase(table.getName().charAt(0)))
                {
                    list.add(table);
                }
            }
        }

        return list;
    }

    public DataTable readData(final Table table, final Connection con) throws SQLException, IOException, JAXBException
    {
        DataTable dataTable = new DataTable();
        dataTable.setTable(table);
        char suffix = 'a';
        try (PreparedStatement stm = con.prepareStatement(String.format("select * from %s", table.getName())))
        {
            ResultSetMetaData metaData = stm.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); ++i)
            {
                Column column = new Column();
                column.setLabel(metaData.getColumnLabel(i));
                column.setType(metaData.getColumnType(i));
                table.addColumn(column);
            }

            try (ResultSet resultSet = stm.executeQuery())
            {
                while(resultSet.next())
                {
                    Row row = new Row();
                    for (Column column : table.getColumns())
                    {
                        switch(column.getType())
                        {
                            case Types.NUMERIC:
                            case Types.DOUBLE:
                            case Types.DECIMAL:
                            case Types.FLOAT:
                                row.add(resultSet.getBigDecimal(column.getLabel()));
                                break;
                            case Types.VARCHAR:
                            case Types.NVARCHAR:
                                row.add(resultSet.getString(column.getLabel()));
                                break;
                            case Types.TIMESTAMP:
                            case Types.OTHER:
                                row.add(resultSet.getTime(column.getLabel()));
                                break;
                            case Types.BLOB:
                            case Types.VARBINARY:
                                Blob blob = resultSet.getBlob(column.getLabel());
                                ByteArrayOutputStream out = new ByteArrayOutputStream();
                                if (blob != null)
                                {
                                    try (InputStream binaryStream = blob.getBinaryStream())
                                    {
                                        byte[] buffer = new byte[1026];
                                        int index;
                                        while((index = binaryStream.read(buffer)) != -1)
                                        {
                                            out.write(buffer, 0, index);
                                        }
                                    }
                                    row.add(out.toByteArray());
                                }
                                else
                                {
                                    row.add(null);
                                }
                                break;
                            case Types.CLOB:
                            case Types.LONGNVARCHAR:
                                CharArrayWriter writer;
                                Clob clob = resultSet.getClob(column.getLabel());
                                if (clob != null && clob.length() > 0)
                                {
                                    try (Reader reader = clob.getCharacterStream())
                                    {
                                        writer = new CharArrayWriter();
                                        char[] buffer = new char[1026];
                                        int length;
                                        while((length = reader.read(buffer)) > -1)
                                        {
                                            writer.write(buffer, 0, length);
                                        }
                                        row.add(writer.toString());
                                    }
                                }
                                else
                                {
                                    row.add(null);
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Not supported type: " + column.getType());
                        }
                    }
                    dataTable.addRow(row);
                    if (dataTable.getRows().size() >= maxRows)
                    {
                        getZipper().write(dataTable, suffix);
                        ++suffix;
                        LOG.info("... wrote part-table.");
                        dataTable = new DataTable();
                        dataTable.setTable(table);
                    }
                }
            }
        }
        getZipper().write(dataTable);
        getZipper().write(table);
        return dataTable;
    }
        
    public ZipWriter getZipper()
    {
        if (zipper == null)
        {
            try
            {
                zipper = new ZipWriter();
            }
            catch(JAXBException e)
            {
                throw new IllegalStateException("Could not create the zip-writer!", e);
            }
        }
        return zipper;
    }

    public void setMaxRows(int maxRows)
    {
        this.maxRows = maxRows == 0 ? MAX_ROWS : maxRows;
    }

}
