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
import java.io.File;

/**
 * Definition of a TSDB file.
 */
public class TSDBDef {

    public static final char MEAN = 'm';
    public static final char SUM = 's';
    public static final char VALUE = 'v';


    public static final char LONG = 'l';
    public static final char DOUBLE = 'd';
    private File file;
    private Object[] defaultRecord;
    private String name;
    private String metadata;

    public static Builder builder() {
        return new Builder();
    }


    private int nblocks;
    private final long[] recordPeriod;
    private final int[] nrecords;
    private final int[] recordWindow;
    private final int nFields;
    private final String mergOPeration;
    private final String fieldTypes;

    private TSDBDef(@Nonnull  String filename,
                    @Nonnull String name,
                    @Nonnull String fieldTypes,
                    @Nonnull String mergOPeration,
                    @Nonnull int[] nrecords,
                    @Nonnull int[] recordWindow,
                    @Nonnull long[] recordPeriod,
                    @Nonnull String metadata) {
        this.nblocks = nrecords.length;
        this.name = name;
        this.file = new File(filename);
        this.nFields = fieldTypes.length();
        this.metadata = metadata;
        if ( recordPeriod.length != nblocks || recordWindow.length != (nblocks-1)) {
            throw new IllegalArgumentException("Number of blocks is not consistent.");
        }
        if (fieldTypes.length() != mergOPeration.length()) {
            throw new IllegalArgumentException("Number of fields is not consistent.");
        }
        if ( !fieldTypes.startsWith("l")) {
            throw new IllegalArgumentException("The first field must be a long to accumlate timestamps.");
        }
        if ( !mergOPeration.startsWith("v") ) {
            throw new IllegalArgumentException("The first field must merge by value (ie overwrite) as it is a timestamp.");
        }
        for(char c: fieldTypes.toCharArray()) {
            if ( !( c == LONG || c == DOUBLE) ) {
                throw new IllegalArgumentException("Fields types are invalid "+fieldTypes);
            }
        }
        for(char c : mergOPeration.toCharArray()) {
            if ( !(c == SUM || c == MEAN || c == VALUE)) {
                throw new IllegalArgumentException("Operations types are invalid "+mergOPeration);
            }
        }
        this.recordPeriod = recordPeriod;
        this.nrecords = nrecords;
        this.recordWindow = recordWindow;
        this.mergOPeration = mergOPeration;
        this.fieldTypes = fieldTypes;
    }

    public int getNBlocks() {
        return nblocks;
    }

    public long getRecordPeriod(int b) {
        return recordPeriod[b];
    }

    public int getNRecords(int b) {
        return nrecords[b];
    }

    /**
     * The number of records this block that make up a record in the next block.
     * @param b
     * @return
     */
    public int getRecordWindow(int b) {
        return recordWindow[b];
    }

    public int getNFields() {
        return nFields;
    }

    public char getFieldMergeOperation(int i) {
        return mergOPeration.charAt(i);
    }

    public char getFieldType(int i) {
        return fieldTypes.charAt(i);
    }


    @Nonnull
    public File getFile() {
        return file;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * Discourage access to this, arrays are mutable.
     * @return
     */
    @Nonnull
    int[] getBlockRecords() {
        return nrecords;
    }

    @Nonnull
    public String getFieldTypes() {
        return fieldTypes;
    }

    @Nonnull
    public String getMetadata() {
        return metadata;
    }


    public static class Builder {
        private String fieldTypes;
        private String mergePolicy;
        private long[] blockPeriod;
        private long[] recordPeriod;
        private String filename;
        private String name;
        private String metadata = "{}";

        /**
         * Set the field types using a string. 1 char per field, l = long, d = double,
         * use {@link TSDBDef.DOUBLE} and {@link TSDBDef.LONG}, the first must be a l to hold the timestamp.
         * @param fieldTypes
         * @return the Builder
         */
        @Nonnull
        public Builder setFelds(@Nonnull String fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * Set the merge policy for the fields, 1 char per field, s = sum, m = mean, v = valie ie overwrite on merge.
         * @param mergePolicy
         * @return the Builder
         */
        @Nonnull
        public Builder setMergePolicy(@Nonnull String mergePolicy) {
            this.mergePolicy = mergePolicy;
            return this;
        }

        /**
         * Set metadata for the file, writen on create, cant be changed once created. Normally used to decrive the data could
         * be whatever the caller wants, within reason.
         * @param metadata
         * @return the Builder
         */
        @Nonnull
        public Builder setMetadata(@Nonnull String metadata) {
            this.metadata = metadata;
            return this;
        }

        /**
         * Set the internal name of the file.
         * @param name
         * @return the Builder
         */
        @Nonnull
        public Builder setName(@Nonnull String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the filename to open.
         * @param fileName
         * @return the Builder
         */
        @Nonnull
        public Builder setFileName(@Nonnull String fileName) {
            this.filename = fileName;
            return this;
        }
        /**
         * Set the amount of time each block and record represents in seconds.
         * For instance
         * blockPeriod 6*3600, 3600*24*7, 3600*24*30, 3600*24*120
         * recordPeriod 5, 60, 3600, 24*3600
         *   block 1 covers the last 6h at a resolution of 5s (ie 6*3600/5  = 4320records)
         *   block 2 covers the last 7d at a resolution of 1m  (ie 3600*24*7/60 = 10080 records)
         *   block 3 covers the last 30d at a resolution of 1h (ie 3600*24*30/3600 = 720 records)
         *   block 4 covers the last 120d at a resolution of 1d (ie 3600*24*120/24*3600 = 120 records)
         * @param blockPeriod
         * @return
         */
        @Nonnull
        public Builder setBlockPeriod(@Nonnull long[] blockPeriod, @Nonnull long[] recordPeriod) {
            this.blockPeriod = blockPeriod;
            this.recordPeriod = recordPeriod;
            return this;
        }


        /**
         * Validate all the properties and if ok build a TSDDef object.
         * @return
         */
        @Nonnull
        public TSDBDef build() {
            if ( mergePolicy == null || fieldTypes == null) {
                throw new IllegalArgumentException("Field types and merge policies must be specified.");
            }
            if ( blockPeriod == null || recordPeriod == null) {
                throw new IllegalArgumentException("Both the block period and record period must be specified.");
            }
            if ( blockPeriod.length != recordPeriod.length) {
                throw new IllegalArgumentException("Both the block period and record period must be the same length.");
            }
            // check that its possible to build a fixed period structure from the input data.
            for(int i = 0; i < blockPeriod.length; i++) {
                if ( blockPeriod[i] < recordPeriod[i] || blockPeriod[i]%recordPeriod[i] != 0) {
                    throw new IllegalArgumentException("Each block period must be greater that the record period and the block period must be a multiple of the record period.");
                }
                if (i > 0) {
                    if ( recordPeriod[i] < recordPeriod[i-1] || recordPeriod[i]%recordPeriod[i-1] != 0) {
                        throw new IllegalArgumentException("Each record period must be an exact multiple of the previous block record peroid.");
                    }
                    if ( recordPeriod[i] > blockPeriod[i-1] ) {
                        throw new IllegalArgumentException("Each record period must less thanthe previous block peroid.");
                    }
                }
            }
            int[] nrecords = new int[blockPeriod.length];
            for ( int i = 0; i < nrecords.length; i++) {
                nrecords[i] = (int)(blockPeriod[i]/recordPeriod[i]);
            }
            int[] recordWindow = new int[blockPeriod.length-1];
            for ( int i =  0; i < nrecords.length-1; i++) {
                recordWindow[i] = (int)(recordPeriod[i+1]/recordPeriod[i]);
            }
            long[] recordPeriodms = new long[recordPeriod.length];
            for (int i = 0; i < recordPeriod.length; i++) {
                recordPeriodms[i] = recordPeriod[i]*1000L;
            }


            return new TSDBDef(filename, name, fieldTypes, mergePolicy, nrecords, recordWindow, recordPeriodms, metadata);
        }
    }

}
