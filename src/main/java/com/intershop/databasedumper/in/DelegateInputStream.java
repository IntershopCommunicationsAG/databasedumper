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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 *  Used for Import from Zip package.
 */
class DelegateInputStream extends InputStream
{
    private static final Logger LOG = LoggerFactory.getLogger(DelegateInputStream.class);
    
    private InputStream delegate;
    
    @Override
    public int available() throws IOException
    {
        return delegate.available();
    }

    @Override
    public void close() throws IOException
    {
        /*
         * Do nothing and keep the delegate-stream open.
         * For a ZipInputStream, this is necessary!
         */
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        return !DelegateInputStream.class.isAssignableFrom(obj.getClass()) && delegate.equals(obj);
    }

    @Override
    public int hashCode()
    {
        return delegate.hashCode();
    }

    @Override
    public synchronized void mark(int arg0)
    {
        delegate.mark(arg0);
    }

    @Override
    public boolean markSupported()
    {
        return delegate.markSupported();
    }

    @Override
    public int read() throws IOException
    {
        return delegate.read();
    }

    @Override
    public int read(byte[] arg0, int arg1, int arg2) throws IOException
    {
        return delegate.read(arg0, arg1, arg2);
    }

    @Override
    public int read(byte[] arg0) throws IOException
    {
        return delegate.read(arg0);
    }

    @Override
    public synchronized void reset() throws IOException
    {
        delegate.reset();
    }

    @Override
    public long skip(long arg0) throws IOException
    {
        return delegate.skip(arg0);
    }

    public DelegateInputStream(InputStream delegate)
    {
        super();
        this.delegate = delegate;
    }

    
}
