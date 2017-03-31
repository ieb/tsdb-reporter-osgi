/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sling.tsdb;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.Arrays;


/**
 * This is a variable size TSDB file containing a header and a number of blocks of data. Each block of data may have multiple differing records, but each record
 * is assumed to be the same size. The length of the file is predefined at first creation.
 */
public class TSDBFile implements AutoCloseable {

    private final String name;
    private final int[] nrecords;
    private final RandomAccessFile file;

    private static String VERSION1_FORMAT = "TSDBv1";
    private static int ENDOFHEADER_MAGIC = 28193746;
    private final long endHeaderPointer;
    private final long recordLength;
    // Not using an [] of enums becuase that would be mutable without a copy, and this must be immutable. Just simpler to use a string.
    private final String recordDef;
    // arbitary metadata in a format the consumer understands.
    private final String metadata;


    public TSDBFile(@Nonnull File f, @Nonnull  String name, @Nonnull  int[] nrecords, @Nonnull String recordDef, @Nonnull String metadata, boolean create) throws IOException {
        this.name = name;
        this.nrecords = nrecords;
        this.recordDef = recordDef;
        this.metadata = metadata;
        if ( !create && !f.exists()) {
            throw new FileNotFoundException("File doesnt exist, cant open without create option "+f.getAbsolutePath());
        }
        if (f.exists()) {
            file = new RandomAccessFile(f, "rw" );
            String format = file.readUTF();
            if ( VERSION1_FORMAT.equals(format)) {
                String fname = file.readUTF();
                int fnr = file.readInt();
                int[] fnrecords = new int[fnr];
                for( int i = 0; i < fnr; i++ ) {
                    fnrecords[i] = file.readInt();
                }
                String frecordDef = file.readUTF();
                recordLength = file.readLong();
                String metadataIgnored = file.readUTF();
                int magic2 = file.readInt();
                this.endHeaderPointer = file.getFilePointer();
                if ( magic2 != ENDOFHEADER_MAGIC) {
                    throw new IOException("Bad header, missing end of header marker");
                }
                if ( !fname.equals(name)) {
                    throw new IOException("Names within file dont match, found "+fname+" expected "+name);
                }
                if ( fnrecords.length != nrecords.length ) {
                    throw new IOException("Number of record blocks dont match found "+fnrecords.length+" expected "+nrecords.length);
                }
                if ( !frecordDef.equals(recordDef)) {
                    throw new IOException("Record defintion does not match found "+frecordDef+" expected "+recordDef);
                }
                if ( recordLength != calcRecordLength(recordDef)) {
                    throw new IOException("Record length does not match found "+recordLength+" expected "+calcRecordLength(recordDef));
                }
            } else {
                throw new IOException("TSDBFile with format ID of "+format+" is not a recognised format.");
            }
        } else {
            file = new RandomAccessFile(f, "rw" );
            recordLength = calcRecordLength(this.recordDef);
            file.writeUTF(VERSION1_FORMAT);
            file.writeUTF(this.name);
            file.writeInt(this.nrecords.length);
            for (int i : this.nrecords) {
                file.writeInt(i);
            }
            file.writeUTF(this.recordDef);
            file.writeLong(recordLength);
            file.writeUTF(this.metadata);
            file.writeInt(ENDOFHEADER_MAGIC);
            endHeaderPointer = file.getFilePointer();
            // fill the file with default Data
            Object[] defaultRecord = buildeDefaultObject(recordDef);
            for ( int i : this.nrecords) {
                for (int j = 0; j < i; j++) {
                    write(defaultRecord);
                }
            }
        }
    }

    @Nonnull
    private String buildRecordDef(@Nonnull Object[] sampleRecord) {
        StringBuilder sb = new StringBuilder();
        for (Object o : sampleRecord) {
            if ( o instanceof Long) {
                sb.append("l");
            } else if ( o instanceof Double ) {
                sb.append("d");
            }
        }
        return sb.toString();
    }
    @Nonnull
    private Object[] buildeDefaultObject(@Nonnull String recordDef) {
        Object[] o = new Object[recordDef.length()];
        for (int i = 0; i < recordDef.length(); i++) {
            switch(recordDef.charAt(i)) {
                case TSDBDef.LONG: o[i] = 0L; break;
                case TSDBDef.DOUBLE: o[i] = 0.0D; break;
            }
        }
        return o;
    }

    private long calcRecordLength(@Nonnull String recordDef) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        for (char c : recordDef.toCharArray()) {
            switch(c) {
                case TSDBDef.LONG: dos.writeLong(1L); break;
                case TSDBDef.DOUBLE: dos.writeDouble(1.0D); break;
            }
        }
        dos.flush();
        baos.flush();
        return baos.toByteArray().length;
    }

    private void check(int block, int record, @Nonnull Object[] data) throws IOException {
        check(block, record);
        if ( !recordDef.equals(buildRecordDef(data))) {
            throw new IOException("Data in wrong format for file.");
        }
    }
    private void check(int block, int record) throws IOException {
        try {
            if (record < 0 ) {
                throw new IOException("Negative records are not allowed.");
            } else if ( record >= nrecords[block] ) {
                throw new IOException("Record exceeds block size.");
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IOException("Block "+block+"does not exist ");
        }
    }

    /**
     * Write a record to block:record of data
     * @param block
     * @param record
     * @param data
     * @throws IOException if location or structure doesnt match the file.
     */
    public void writeRecord(int block, int record, @Nonnull Object[] data) throws IOException {
        check(block, record, data);
        seek(block,record);
        write(data);
    }

    private void write(@Nonnull Object[] data) throws IOException {
        for (Object o : data) {
         if (o instanceof Long) {
             file.writeLong((long)o);
         } else if ( o instanceof Double ) {
             file.writeDouble((double)o);
         }
        }
    }

    /**
     * read a record.
     * @param block
     * @param record
     * @return the record object[]
     * @throws IOException
     */
    @Nonnull
    public Object[] readRecord(int block, int record) throws IOException {
        check(block, record);
        seek(block,record);
        Object[] data = new Object[recordDef.length()];
        read(data, 0);
        return data;

    }

    private int read(@Nonnull Object[] data, int offset) throws IOException {
        for (char c : recordDef.toCharArray()) {
            if ( c =='l' ) {
                data[offset] = file.readLong();
            } else if ( c == 'd') {
                data[offset] = file.readDouble();
            }
            offset++;
        }
        return offset;
    }

    /**
     * Read an entire block, in row column order.
     * @param block the block number
     * @return an Object[] containign each record as a linear array, ie row column order where the row is the record.
     * @throws IOException
     */
    @Nonnull
    public Object[] readBlock(int block) throws IOException {
        if (block < 0 || block > nrecords.length) {
            throw new IOException("BLock out of range");
        }
        seek(block,0);
        Object[] data = new Object[nrecords[block]*recordDef.length()];
        int j = 0;
        for (int i = 0; i < nrecords[block]; i++) {
            j = read(data, j);
        }
        return data;

    }

    private void seek(int block, int record) throws IOException {
        long p = endHeaderPointer;
        for( int i = 0; i < block; i++) {
            p =  p + nrecords[i]*recordLength;
        }
        p = p+record*recordLength;
        file.seek(p);
    }


    @Nonnull
    public String info() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: ").append(this.name).append("\n");
        sb.append("nrecords: ").append(Arrays.toString(this.nrecords)).append("\n");
        sb.append("recordLength: ").append(this.recordLength).append(" bytes\n");
        sb.append("recordDef: ").append(this.recordDef).append("\n");
        sb.append("endOfHeaderOffset: ").append(this.endHeaderPointer).append("\n");
        sb.append("metadata: ").append(this.metadata);
        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        file.close();
    }
}

