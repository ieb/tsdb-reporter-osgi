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
import javax.annotation.Nullable;
import java.io.IOException;

/**
 * An updater is required to update a TSDBFile. It will open the file on creation but close it again once created/checked and
 * will only open it when modifications are required. If close is not called, the file will remain open. Best to call close
 * if there is a risk of a large number of tsdb files being used. Once created, the structure of a tsdb file cant be changed.
 */
public class TSDBUpdater implements AutoCloseable {


    /**
     * The time when the next write will happen for each block.
     */
    private final long[] nextWrite;

    /**
     * The record number where the next write will happen for each block
     */
    private final int[] record;

    /**
     * The live data recieving updates not written to file yet.
     */
    private Object[] liveData;

    private final TSDBDef def;
    /**
     * The file, must call get file to open.
     */
    private TSDBFile tsdbFile;

    /**
     * Create an updater based on the file definition.
     * @param def the definition.
     */
    public TSDBUpdater(@Nonnull  TSDBDef def) throws IOException {
        this.def = def;
        this.record = new int[def.getNBlocks()];
        this.nextWrite = new long[record.length];
        // live data gets initialised on first merge must be null to get a valid first reading.
        getFile();
        try {
            close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    /**
     * Get the TFDBFile opening if necessary. Will create the file if not present.
     * @return the tsdbfile ready for reading or writing. The
     * @throws IOException when unable to open the file.
     */
    @Nonnull
    private TSDBFile getFile() throws IOException {
        if (tsdbFile == null) {
            tsdbFile = new TSDBFile(def.getFile(), def.getName(), def.getBlockRecords(), def.getFieldTypes(), def.getMetadata(), true);
        }
        return tsdbFile;
    }

    /**
     * close the tsdb file, must be called to avoid file handle exhaustion in a large system.
     * This may result in slower operation. If a problem consider putting more records into 1 file.
     * @throws IOException when unable to close.
     */
    public void close() throws IOException {
        if (tsdbFile != null) {
            try {
                tsdbFile.close();
            } catch (Exception e ) {
                throw new IOException(e);
            }
            tsdbFile = null;
        }
    }



    /**
     * Update with data at time tnow. Ideally this should be called at least once per
     * @param tnow time when the data was created.
     * @param data the data record.
     */
    public void update(long tnow, @Nonnull  Object[] data) throws IOException {
        // accumulate data until time to write.
        updateLive(data);
        for(int b = 0; b < def.getNBlocks(); b++) {
            while( tnow >= nextWrite[b] ) {
                getFile().writeRecord(b,record[b],accumulateFor(b));
                incrementRecord(b);
                nextWrite[b] = nextWrite[b]+def.getRecordPeriod(b);
            }
        }
    }

    /**
     * Increment record number for bloc b
     * @param b block number.
     */
    private void incrementRecord(int b) {
        record[b] = (record[b]+1)%def.getNRecords(b);
    }

    /**
     * Accumulate for block b from the previous bloc (b-1). If b is 0, then accumulate from teh current live store.
     * @param b block number.
     * @return an accumulated object array with the first element set to the current block time.
     * @throws IOException if records cant be read.
     */
    @Nonnull
    private Object[] accumulateFor(int b) throws IOException {
        int from = b-1;
        if (from < 0) {
            liveData[0] = nextWrite[b];
            return liveData;
        } else {
            // accumulate n records from the previous block
            Object[] acc = null;
            for(int i = record[from]-def.getRecordWindow(from); i < record[from]; i++) {
                int rn = i;
                if (rn < 0 )  {
                    rn = rn+def.getNRecords(from); // assuming config is good and recordWindow < nrecords
                }
                acc = merge(acc, getFile().readRecord(from,rn) ,def.getRecordWindow(from));
            }
            if ( acc != null) {
                acc[0] = nextWrite[b]; // set the timestamp that is always field 0.
            }
            return acc;
        }
    }


    /**
     * Update the current live data set with new data.
     * @param data a data record.
     */
    private void updateLive(@Nonnull  Object[] data) {
        liveData = merge(liveData, data, 2);
    }

    /**
     * Merge data into an accumulator (acc) expecting nmerges to happen.
     * @param acc an record to accuminate into, may be null in which case it will be initialised to the default record.
     * @param data the data record
     * @param nmerges the number of contributing records, for means.
     * @return the merged record.
     */
    @Nonnull
    private Object[] merge(@Nullable  Object[] acc, @Nonnull  Object[] data, int nmerges) {
        if (acc == null) {
            acc = new Object[data.length];
            System.arraycopy(data,0,acc,0,data.length);
        } else {
            for (int i = 0; i < def.getNFields(); i++) {
                switch(def.getFieldMergeOperation(i)) {
                    case TSDBDef.MEAN: // mean
                        switch(def.getFieldType(i)) {
                            case TSDBDef.DOUBLE: acc[i] = (((double)acc[i])+((double)data[i]))/((double)nmerges); break;
                            case TSDBDef.LONG: acc[i] = (((long)acc[i])+((long)data[i]))/((long)nmerges); break;
                        }
                        break;
                    case TSDBDef.SUM: // sum
                        switch(def.getFieldType(i)) {
                            case TSDBDef.DOUBLE: acc[i] = ((double)acc[i])+((double)data[i]); break;
                            case TSDBDef.LONG: acc[i] = ((long)acc[i])+((long)data[i]); break;
                        }
                        break;
                    case TSDBDef.VALUE: // value
                        acc[i] = data[i];
                        break;
                }
            }
        }
        return acc;
    }


}
