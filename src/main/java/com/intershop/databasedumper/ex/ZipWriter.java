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

import com.intershop.databasedumper.DatabaseDumper;
import com.intershop.databasedumper.data.DataTable;
import com.intershop.databasedumper.meta.Row;
import com.intershop.databasedumper.meta.Table;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This class zip the data files
 * to a package.
 */
public class ZipWriter implements AutoCloseable
{
    protected ZipOutputStream out = null;
    protected Marshaller marshaller = null;
    protected XMLStreamWriter writer = null;
    protected Table table = null;

    public ZipWriter(ZipOutputStream out, Table table) throws JAXBException, IOException, XMLStreamException
    {
        this.out = out;
        this.table = table;
        
        JAXBContext jc = JAXBContext.newInstance(Table.class);
        Marshaller metaDataMarshaller = jc.createMarshaller();
        metaDataMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        metaDataMarshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "http://www.w3.org/2001/XMLSchema-Instance");
        ZipEntry metaEntry = new ZipEntry(DatabaseDumper.META_DIR_NAME + "/" + table.getName() + ".xml");
        out.putNextEntry(metaEntry);
        metaDataMarshaller.marshal(table, new StreamResult(out));
        out.closeEntry();
        out.flush();
        
        jc = JAXBContext.newInstance(Row.class);
        marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        // marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "http://www.w3.org/2001/XMLSchema-Instance");
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        ZipEntry entry = new ZipEntry(table.getName() + ".xml");
        out.putNextEntry(entry);
        
        writer = XMLOutputFactory.newFactory().createXMLStreamWriter(out);
        writer.writeStartDocument();
        writer.writeStartElement("dataTable");
    }

    public void write(Row row) throws JAXBException, IOException
    {
        marshaller.marshal(row, writer);
    }
    
    public Table getTable()
    {
        return table;
    }

    @Override
    public void close() throws Exception
    {
        writer.writeEndDocument();
        writer.close();
        out.closeEntry();
        out.flush();
    }
}
