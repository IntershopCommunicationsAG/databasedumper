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
import com.intershop.databasedumper.meta.Table;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * This class zip the data files
 * to a package.
 */
public class ZipWriter
{
    private ZipOutputStream out;
    private Marshaller marshaller;

    public ZipWriter() throws JAXBException
    {
        super();
        JAXBContext jc = JAXBContext.newInstance(DataTable.class);
        marshaller = jc.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    }

    public void write(final DataTable dataTable) throws JAXBException, IOException
    {
        write(dataTable, null);
    }

    public void write(final DataTable dataTable, final Character suffix) throws JAXBException, IOException
    {
        StringBuilder name = new StringBuilder();
        name.append(dataTable.getTable().getName());
        if (suffix != null)
        {
            name.append('_').append(suffix.charValue());
        }
        name.append(".xml");

        ZipEntry entry = new ZipEntry(name.toString());
        out.putNextEntry(entry);
        marshaller.marshal(dataTable, new StreamResult(out));
        out.closeEntry();
        out.flush();
        System.gc();
    }

    public void write(final Table table) throws IOException, JAXBException
    {
        ZipEntry metaEntry = new ZipEntry(DatabaseDumper.META_DIR_NAME + "/" + table.getName() + ".xml");
        out.putNextEntry(metaEntry);
        marshaller.marshal(table, new StreamResult(out));
        out.closeEntry();
        out.flush();
        System.gc();
    }

    public void setOut(ZipOutputStream out)
    {
        this.out = out;
    }

    public void setMarshaller(Marshaller marshaller)
    {
        this.marshaller = marshaller;
    }
}
