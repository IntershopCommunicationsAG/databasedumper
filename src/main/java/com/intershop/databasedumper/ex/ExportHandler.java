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

import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.zip.ZipOutputStream;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.intershop.xml.ns.enfinity._7_1.xcs.impex.ObjectFactory;
import com.intershop.xml.ns.enfinity._7_1.xcs.impex.TableStructure;

/**
 * This is the main class for the export
 * functionlity.
 * It analyses tables and store this information
 * in separate xml data files.
 */
public class ExportHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(ExportHandler.class);

    private static final int MAX_ROWS = 100000;

    private int maxRows = MAX_ROWS;
    
    private ZipWriter zipper;
    
    protected ZipOutputStream out = null;

    public ExportHandler(ZipOutputStream out, int maxRows) {
        this.out = out;
        this.maxRows = maxRows <= 0 ? MAX_ROWS : maxRows;
	}

	public List<TableStructure> readTableNames(final Connection con, final String scheme) throws SQLException
    {    
        List<TableStructure> list = new ArrayList<>();

        System.out.println("Catalog: " + con.getCatalog());
        System.out.println("Schema: " + con.getSchema());

        try (ResultSet tables = con.getMetaData().getTables(null, scheme, null, new String[] { "TABLE" }))
        {
            while(tables.next())
            {
                TableStructure table = new ObjectFactory().
                System.out.println("CAT: " + tables.getString("TABLE_CAT") + ", SCHEM: " + tables.getString("TABLE_SCHEM") + ", NAME: " + tables.getString("TABLE_NAME") + ", TYPE: " + tables.getString("TABLE_TYPE") + ", REMARKS: " + tables.getString("REMARKS"));

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

        System.out.println("Catalog: " + con.getCatalog());
        System.out.println("Schema: " + con.getSchema());
        ResultSet rSet = con.getMetaData().getSchemas(con.getCatalog(), null);
        while(rSet.next())
        {
            System.out.println("TABLE_SCHEM: " + rSet.getString("TABLE_SCHEM") + ", TABLE_CATALOG: " + rSet.getString("TABLE_CATALOG"));
        }

        try (ResultSet tables = con.getMetaData().getTables(con.getCatalog(), con.getSchema(), null, new String[] { "TABLE" }))
        {
            while(tables.next())
            {
                Table table = new Table();
                System.out.println("CAT: " + tables.getString("TABLE_CAT") + ", SCHEM: " + tables.getString("TABLE_SCHEM") + ", NAME: " + tables.getString("TABLE_NAME") + ", TYPE: " + tables.getString("TABLE_TYPE") + ", REMARKS: " + tables.getString("REMARKS"));

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
        // create a data type
        // DataTable dataTable = new DataTable();
        
        // set data types
        dataTable.setTable(table);
        int suffix = 0;
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

            TimeZone tz = TimeZone.getTimeZone("UTC");
            Calendar cal = Calendar.getInstance(tz);
            
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
                            case Types.BIT:
                            case Types.BIGINT:
                            case Types.SMALLINT:
                            case Types.INTEGER:
                            case Types.TINYINT:
                                row.add(resultSet.getBigDecimal(column.getLabel()));
                                break;
                            case Types.CHAR:
                            case Types.VARCHAR:
                            case Types.NVARCHAR:
                                row.add(resultSet.getString(column.getLabel()));
                                break;
                            case Types.TIMESTAMP:
                            case Types.TIME:
                                Date resultDate = resultSet.getDate(column.getLabel(), cal);
        						Time resultsTime = resultSet.getTime(column.getLabel(), cal);
        						Date d = null;
        						if (null != resultDate && null != resultsTime)
								{
									d = new Date(resultDate.getTime() + resultsTime.getTime());
									LOG.debug(column.getType() + ": " + resultDate + "/" + resultsTime + " Java: " + resultDate.getTime()
											+ "/" + d);
								} else {
									LOG.debug("Date or time is null");
								}
                            	row.add(d);
                                break;
                            case Types.BLOB:
                            case Types.BINARY:
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
                            case Types.OTHER:
                            default:
							throw new IllegalArgumentException(
									"Unsupported type: " + table.getName() + "." + column.getLabel() + "("
											+ java.sql.JDBCType.valueOf(column.getType()).getName() + ")");
                        }
                    }
                    // dataTable.addRow(row);
                    // if (dataTable.getRows().size() >= maxRows)
                    // {
                	// write a new chunk of the table to the target zip
                    getZipper(table).write(row);
                    // increment the suffic
                    ++suffix;
                    LOG.info("... wrote table chunk.");
                    // create a new data table
                    dataTable = new DataTable();
                    // set the known meta data information
                    dataTable.setTable(table);
                    // }
                }
                getZipper(table).close();
            }
            catch(Exception e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
//        getZipper().write(dataTable);
//        // check data types of all columns of the table
//        if (validateType(table))
//        {
//        	// write table metadata file after writing data
//        	getZipper().write(table);
//        }
        return dataTable;
    }
        
    protected boolean validateType(Table table) {
    	boolean result = true;
    	
    	ColumnTypeComperator columnTypeComparater = new ColumnTypeComperator();
    	
    	for (Column column : table.getColumns())
    	{
    		if (!columnTypeComparater.isKnownType(column.getType()))
    		{
    			result = false;
				LOG.warn("Unsupported type: {}.{}({})", table.getName(), column.getLabel(),
						java.sql.JDBCType.valueOf(column.getType()).getName());
    		}
    	}
    	
    	return result;
	}

	public ZipWriter getZipper(Table table)
    {
        if (zipper == null || !zipper.getTable().equals(table))
        {
            try
            {
                zipper = new ZipWriter(out, table);
            }
            catch(JAXBException | IOException | XMLStreamException e)
            {
                throw new IllegalStateException("Could not create the output zip writer!", e);
            }
        }
        return zipper;
    }
}
