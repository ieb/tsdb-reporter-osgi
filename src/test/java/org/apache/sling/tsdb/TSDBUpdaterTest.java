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

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by boston on 31/03/2017.
 */
public class TSDBUpdaterTest {

    @Test
    public void testUpdate() throws IOException {
        File f = new File("updatertest.tsdb");
        if ( f.exists()) {
            f.delete();
        }

        long blockPeriod[] = new long[] { 10, 300, 2000};
        long recordPeriod[] = new long[] { 1, 5, 20};
        TSDBDef def = TSDBDef.builder()
                .setFileName("updatertest.tsdb")
                .setFelds("ldldldl")
                .setMergePolicy("vssmmvv")
                .setName("testupdater")
                .setBlockPeriod(blockPeriod, recordPeriod)
                .setMetadata("{ normally a big block of json }")
                .build();
        TSDBUpdater tsdbUpdater = new TSDBUpdater(def);
        Object data[] = new Object[]{
                1L,
                222.2D,
                300L,
                444.4D,
                500L,
                666.6D,
                700L
        };
        SecureRandom rand = new SecureRandom();
        for(int i = 0; i < 10000; i++) {
            data[0] = i*256;
            System.err.println(Arrays.toString(data));
            tsdbUpdater.update(i*256, data);
            for (int j = 2; j < data.length; j+=2) {
                data[j] = (long)data[j]+rand.nextInt(11)-5;
            }
            for (int j = 1; j < data.length; j+=2) {
                data[j] = (double)data[j]+(rand.nextDouble()*11.0D)-5.0D;
            }

            tsdbUpdater.close();
        }


        // Then read and dump the blocks.
        TSDBFile tsdbFile = new TSDBFile(def.getFile(), def.getName(), def.getBlockRecords(), def.getFieldTypes(), def.getMetadata(), false);
        System.err.println(tsdbFile.info());
        int[] blockRecrds = def.getBlockRecords();
        for (int bn = 0; bn < blockRecrds.length; bn++) {
            System.err.println("Block "+bn);
            for ( int i = 0; i < blockRecrds[bn]; i++) {
                System.err.println(Arrays.toString(tsdbFile.readRecord(bn, i)));
            }
        }


    }
}
