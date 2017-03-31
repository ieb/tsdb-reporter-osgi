This provides a OSGi bundle that has a component storing Dropwizard Metrics in TSDB files on the local disk. These files
may later be retrieved for reporting. 


[![Build Status](https://travis-ci.org/ieb/tsdb-reporter-osgi.svg?branch=master)](https://travis-ci.org/ieb/tsdb-reporter-osgi)

# Quick Setup for the impatient

Install the bundle. Thats it.


# What it does

The aim of this bundle is to save metrics onto locally mounted disk. Those files should contain historical metrics that can be 
 extracted and ingesting into more capable reporting tools like InfluxDB+Grafana or Ganglia. The files are a custom format, 
  based on the same concepts as Round Robin Database files. Each file contains a header, and a number of blocks of fixed
  size and number records. Data is written to the first block in a circular fashion, feeding the subsequent block at fixed
  intervals. This chained block structure allows long periods of time to be recorded in a single file with a fixed size. Depending
  on the configuration the granularity of the records increases through each block. For instance, expressed in seconds
  
  
    blockPeriod 6*3600, 3600*24*7, 3600*24*30, 3600*24*120
    recordPeriod 5, 60, 3600, 24*3600
    block 1 covers the last 6h at a resolution of 5s (ie 6*3600/5  = 4320records)
    block 2 covers the last 7d at a resolution of 1m  (ie 3600*24*7/60 = 10080 records)
    block 3 covers the last 30d at a resolution of 1h (ie 3600*24*30/3600 = 720 records)
    block 4 covers the last 120d at a resolution of 1d (ie 3600*24*120/24*3600 = 120 records)
  
  

Data is written to the files using standard Java DataInputStream format, which ensures records appear at fixed offsets. To read, 
convert and ingest the classes in this bundle will be required. The files format is versioned by the first URF8 encoded string in 
 the file to allow future changes.

Integration with Dropwizard Metrics is achieved via a DropWizard Metrics reporter.

# Foramt detail

For full current detaisl of the format see TFDBFile. THis is a summary

    Version Marker
    Header
    End Header Marker
    Block
    Block
    Block
    ....
    Block

   * _Version marker_ is a UTF8 String, Version 1 is TSDBv1
   * _Header_ contains a number of fields defining the record format, number of records per block and number of blocks as
    well as some metadata.
   * _End Header Marker_ is an Integer 28193746
   * _Block_ is made up of a fixed number of records for each block, with a common format of each record.
   THe first entry in a record is always a long containing the timesamp in ms when the record was created. The remaining 
   fields may be Long or Double fields.
   * Each _Block_ contains a different number of records. All records are identical in format.
   * Each _Block_ is written as a circular buffer.
    

# Why not RRD

RRDTool is format supported by native libraries and tools. There is a Java RRD library, but the format it writes is 
not compatable with RRDTool so the files cant be read.  That format and that library do not have universal adoption.
There are several interface libraries, all of which require the native commands to be executed. For that reason, and to 
manage the risks of a changing format, a custom format was chosen. If a stable and widely accepted alternative format is 
avalable that fits the needs of this bundle is availablem then the custom format should be deprecated. 