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

package com.intershop.databasedumper.meta;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Table row.
 */
@XmlRootElement
public class Row
{
    private List<Object> element = new ArrayList<>();

    public Row()
    {
        super();
    }

    public List<Object> getElement()
    {
        return element;
    }

    public void setElement(List<Object> element)
    {
        this.element = element;
    }

    public boolean add(Object e)
    {
        return element.add(e);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((element == null) ? 0 : element.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Row other = (Row)obj;
        if (element == null)
        {
            if (other.element != null)
                return false;
        }
        else if (!element.equals(other.element))
            return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "Row [element=" + element + "]";
    }

}
