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

import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Column;
import com.intershop.databasedumper.meta.Row;
import com.intershop.databasedumper.meta.Table;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.math.BigDecimal;

/**
 * Reads the XML object with all required table information.
 * This object is stored in the export package and used
 * for pre verification before the import runs.
 */
class DataTableParser extends DefaultHandler
{

    private static final String ROWS = "rows";
    private static final String TABLE = "table";
    private static final String COLUMNS = "columns";
    private static final String ELEMENT = "element";
    
    private DataTable dataTable = new DataTable();
    private Row row;
    private Table table;
    private Column column;
    private String content;
    private String type;

    @Override
    public void characters(char[] arg0, int arg1, int arg2) throws SAXException
    {        
        String newOne = new String(arg0, arg1, arg2);
        if (content == null)
        {
            content = newOne;
        }
        else
        {
            content += newOne;
        }
        content = content.trim();        
    }

    @Override
    public void endElement(String arg0, String arg1, String arg2) throws SAXException
    {
        switch(arg2)
        {
            case ROWS:
                getDataTable().addRow(row);
                break;
            case TABLE:
                getDataTable().setTable(table);
                break;
            case COLUMNS:
                table.addColumn(column);
                break;
            case ELEMENT:
                if (content == null)
                {
                    row.add(null);
                }
                else
                {
                    switch(type)
                    {
                        case "string":
                            row.add(content);
                            break;
                        case "decimal":
                            row.add(new BigDecimal(content));
                            break;
                        case "dateTime":
                            try
                            {
                                try
                                {
                                    XMLGregorianCalendar calendar = DatatypeFactory.newInstance()
                                                    .newXMLGregorianCalendar(content);
                                    row.add(calendar);
                                }
                                catch(IllegalArgumentException e)
                                {
                                    e.printStackTrace();
                                    System.err.println("---");
                                    System.err.println(content);
                                    throw e;
                                }
                            }
                            catch(DatatypeConfigurationException e)
                            {
                                throw new SAXException(e);
                            }
                            break;
                        case "base64Binary":
                            row.add(content.getBytes());
                            break;
                        default:
                            throw new IllegalStateException("Unhandled type: " + type);
                    }
                }
                break;
            case "label":
                column.setLabel(content);
                break;
            case "type":
                column.setType(Integer.valueOf(content));
                break;
            case "name":
                table.setName(content);
                break;
            case "xsi:dataTable":
            case "dataTable":
                // That's all! Work should be finished here! :)
                break;
            default:
                throw new IllegalStateException("Unhandled tag: " + arg2);
        }
        content = null;
    }

    @Override
    public void startElement(String arg0, String arg1, String arg2, Attributes arg3) throws SAXException
    {
        switch(arg2)
        {
            case ROWS:
                row = new Row();
                break;
            case TABLE:
                table = new Table();
                break;
            case COLUMNS:
                column = new Column();
                break;
            case ELEMENT:
                String xsiType = arg3.getValue("xsi:type");
                if (xsiType != null)
                {
                    type = xsiType.substring(3);
                }
                else if ("true".equals(arg3.getValue("xsi:nil")))
                {
                    content = null;
                }
                break;
            default:
                // Ignore
        }
    }

    public DataTable getDataTable()
    {
        return dataTable;
    }
}
