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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Exercises the TSDBFile class.
 */
public class TSDBFileTest {


    @Test
    public void test() throws Exception {
        int[] records =  { 3600*24/5,60*24*7,24*30*6 }; // Once every 5s for 24h, once a miniute for 7 days, once an hour for 6 months. == 205K of data compressed.
        File f = new File("test.tsdb");
        if ( f.exists()) {
            f.delete();
        }
        String recordDef = "ldddd";
        TSDBFile tsdbFile = new TSDBFile(f,"testing", records, recordDef, "{}", true);
        for (int i = 0; i < records.length; i++) {
            for ( int j = 0; j < records[i]; j++) {
                Object[] data = new Object[]{(long) i * j, 1.0 * i * j, 2.2 * i * j, 3.3 * i * j, 4.4 * i * j};
                tsdbFile.writeRecord(i, j, data);
            }
        }
        tsdbFile.close();
        tsdbFile = new TSDBFile(f,"testing", records, recordDef, "{}", false);
        for (int i = records.length-1; i >= 0; i--) {
            for ( int j = records[i]-1; j > records[i]; j--) {
                Object[] data = new Object[]{(long) i * j, 1.0 * i * j, 2.2 * i * j, 3.3 * i * j, 4.4 * i * j};
                Object[] fileData = tsdbFile.readRecord(i, j);
                Assert.assertArrayEquals(data, fileData);
            }
        }
        Object[] fileDataExtract = new Object[5];
        for (int i = records.length-1; i >= 0; i--) {
            Object[] fileDataBlock = tsdbFile.readBlock(i);
            for ( int j = records[i]-1; j > records[i]; j--) {
                Object[] data = new Object[]{(long) i * j, 1.0 * i * j, 2.2 * i * j, 3.3 * i * j, 4.4 * i * j};
                System.arraycopy(fileDataBlock, j * 5, fileDataExtract, 0, fileDataExtract.length);
                Assert.assertArrayEquals(data, fileDataExtract);
            }
        }
        for (int i = 0; i < records.length; i++) {
            int j = 5;
            Object[] data = new Object[]{(long)5 * i * j, 11.0 * i * j, 12.2 * i * j, 13.3 * i * j, 14.4 * i * j};
            tsdbFile.writeRecord(i, j, data);
        }
        for (int i = records.length-1; i >= 0; i--) {
            for ( int j = records[i]-1; j > records[i]; j--) {
                Object[] fileData = tsdbFile.readRecord(i, j);
                if ( j == 5) {
                    Object[] data = new Object[]{(long)5 * i * j, 11.0 * i * j, 12.2 * i * j, 13.3 * i * j, 14.4 * i * j};
                    Assert.assertArrayEquals(data, fileData);
                } else {
                    Object[] data = new Object[]{(long) i * j, 1.0 * i * j, 2.2 * i * j, 3.3 * i * j, 4.4 * i * j};
                    Assert.assertArrayEquals(data, fileData);
                }
            }
        }
        tsdbFile.close();


    }
}
