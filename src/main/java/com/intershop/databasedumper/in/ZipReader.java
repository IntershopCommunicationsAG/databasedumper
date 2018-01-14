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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

import com.intershop.databasedumper.DatabaseDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Table;

public class ZipReader
{
    private static final Logger LOG = LoggerFactory.getLogger(ZipReader.class);

    private Unmarshaller unmarshaller;
    private File file;

    public ZipReader() throws JAXBException
    {
        super();
        JAXBContext jc = JAXBContext.newInstance(DataTable.class);
        unmarshaller = jc.createUnmarshaller();
    }

    public Set<String> getTableNames() throws IOException
    {
        Set<String> result = new TreeSet<>();
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(Paths.get(file.getAbsolutePath()), StandardOpenOption.READ)))
        {
            ZipEntry entry;
            while((entry = in.getNextEntry()) != null)
            {
                String name = entry.getName();
                if (name.startsWith(DatabaseDumper.META_DIR_NAME))
                {
                    name = name.substring(DatabaseDumper.META_DIR_NAME.length() + 1, name.length() - ".xml".length());
                    result.add(name);
                }
                in.closeEntry();
            }
        }
        return result;
    }

    public void setFile(File file)
    {
        this.file = file;
    }

    public List<Table> getTables() throws IOException, JAXBException
    {
        List<Table> result = new LinkedList<>();
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(Paths.get(file.getAbsolutePath()), StandardOpenOption.READ)))
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
                    Table table = (Table)unmarshaller
                                    .unmarshal(new InputSource(new ByteArrayInputStream(outStream.toByteArray())));
                    result.add(table);
                }
                in.closeEntry();
            }
        }
        return result;
    }

    public void forEachDataTable(final ImportHandler handler, final ConnectionFactory conFactory)
                    throws IOException, SQLException, ParserConfigurationException, SAXException
    {
        SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(Paths.get(file.getAbsolutePath()), StandardOpenOption.READ)))
        {
            ZipEntry entry;
            while((entry = in.getNextEntry()) != null)
            {
                String name = entry.getName();
                if (!name.startsWith(DatabaseDumper.META_DIR_NAME))
                {
                    LOG.info("Preparing zip-entry {}.", name);
                    DataTableParser dt = new DataTableParser();
                    parser.parse(new DelegateInputStream(in), dt);
                    DataTable dataTable = dt.getDataTable();
                    try (Connection con = conFactory.create())
                    {
                        handler.writeData(dataTable, con);
                    }

                }
                in.closeEntry();
                System.gc();
            }
        }
    }

}
