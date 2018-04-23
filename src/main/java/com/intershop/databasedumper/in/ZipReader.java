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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.intershop.databasedumper.DatabaseDumper;
import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Table;

public class ZipReader
{
    private static final Logger LOG = LoggerFactory.getLogger(ZipReader.class);

    private Unmarshaller tableUnmarshaller;
//    private Unmarshaller dataTableUnmarshaller;
    private File file;

    public ZipReader(File importFile) throws JAXBException
    {
        JAXBContext jc = JAXBContext.newInstance(Table.class);
        tableUnmarshaller = jc.createUnmarshaller();
//        JAXBContext jc2 = JAXBContext.newInstance(DataTable.class);
//        dataTableUnmarshaller = jc2.createUnmarshaller();
        this.file = importFile;
    }

    public Set<String> getTableNames() throws IOException
    {
        Set<String> result = new TreeSet<>();
        try {
			List<Table> tables = getTables();
			for(Table t : tables)
			{
				result.add(t.getName());
			}
		} catch (JAXBException e) {
			LOG.error("Could not read all table names from input file.", e);
		}
        return result;
    }

    /**
     * Read all table meta information from the data ZIP file. 
     * @return list of all Table meta data entries 
     * @throws IOException if file reading fails
     * @throws JAXBException if content reading fails
     */
    public List<Table> getTables() throws IOException, JAXBException
    {
        List<Table> result = new LinkedList<>();
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(file)))
        {
            ZipEntry entry ;
            while((entry = in.getNextEntry()) != null)
            {
                String name = entry.getName();
                if (name.startsWith(DatabaseDumper.META_DIR_NAME))
                {
                    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
                    int read = -1;
                    byte[] buffer = new byte[1028 * 3];

                    while((read = in.read(buffer, 0, buffer.length)) != -1)
                    {
                        outStream.write(buffer, 0, read);
                    }
                    Table table = (Table)tableUnmarshaller
                                    .unmarshal(new InputSource(new ByteArrayInputStream(outStream.toByteArray())));
                    result.add(table);
                }
                in.closeEntry();
            }
        }
        return result;
    }

    public void importDataTables(final ImportHandler handler, final ConnectionFactory conFactory)
                    throws IOException, SQLException, ParserConfigurationException, SAXException
    {
    	// open the data file
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(file)))
        {
        	// create a new sax parser instance
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            
            ZipEntry entry;
            // read each tables data file 
            while((entry = in.getNextEntry()) != null)
            {
            	// get the name of the ZIP entry
                String name = entry.getName();
                // check if entry is NOT from meta information sub folder
                // only data files are processed
                if (!name.startsWith(DatabaseDumper.META_DIR_NAME))
                {
                    LOG.info("Reading table meta data file '{}'.", name);
                    // create a new data entries parser
                    DataTableParser dt = new DataTableParser();
                    // parse the data from the entries input stream
                    parser.parse(new DelegateInputStream(in), dt);
                    // get the data table representation from the parser
                    DataTable dataTable = dt.getDataTable();
//                    DataTable dataTable = (DataTable) dataTableUnmarshaller.unmarshal(new DelegateInputStream(in));
                    // get a connection from the factory
                    try (Connection con = conFactory.create())
                    {
                    	// write data to the connection
                        handler.writeImportData(dataTable, con);
                    }
                }
                // close the zip entry
                in.closeEntry();
            }
//        } catch (JAXBException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
    }

}
