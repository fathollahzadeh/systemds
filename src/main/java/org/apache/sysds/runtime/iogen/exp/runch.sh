#!/usr/bin/env bash
#echo 3 > /proc/sys/vm/drop_caches && sync
#sleep 200

# Set properties
LOG4JPROP='/home/sfathollahzadeh/Documents/GitHub/systemds/scripts/perftest/conf/log4j-off.properties'

sample_raw_fileName="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb_sample_150.raw"
sample_frame_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb_sample_150.frame"
sample_fram_nrows=150
sample_delimiter="\t"
schema_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb.schema";
data_file_name="/media/sfathollahzadeh/Windows/saeedData/NestedDatasets/IMDB/imdb.data"

java -Dlog4j.configuration=file:"$LOG4JPROP" -cp /home/sfathollahzadeh/Documents/GitHub/systemds/target/SystemDS.jar:/home/sfathollahzadeh/Documents/GitHub/systemds/target/lib/* org.apache.sysds.runtime.iogen.exp.GIONestedExperiment $sample_raw_fileName $sample_frame_file_name $sample_fram_nrows $sample_delimiter $schema_file_name $data_file_name
