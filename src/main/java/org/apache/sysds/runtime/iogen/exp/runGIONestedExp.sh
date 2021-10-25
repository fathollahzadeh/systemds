#!/usr/bin/env bash

# Set properties
LOG4JPROP='/home/sfathollahzadeh/Documents/GitHub/systemds/scripts/perftest/conf/log4j-off.properties'
root_data_path="/home/sfathollahzadeh/Dataset"
jar_file_path="/home/sfathollahzadeh/Documents/GitHub/systemds/target/SystemDS.jar"
lib_files_path="/home/sfathollahzadeh/Documents/GitHub/systemds/target/lib/*"
home_log="/home/sfathollahzadeh/Dataset/LOG"
sep="_"
declare -a  datasets=("imdb")

BASE_SCRIPT="java\
            -Dlog4j.configuration=file:$LOG4JPROP\
            -XX:-UseGCOverheadLimit\
            -XX:+UseConcMarkSweepGC\
            -Xms1g\
            -Xmx14g\
            -cp\
             $jar_file_path:$lib_files_path\
             org.apache.sysds.runtime.iogen.exp.GIONestedExperiment\
             "

for d in "${datasets[@]}"; do
  ./resultPath.sh $home_log $d
  data_file_name="$root_data_path/$d/$d.data"
  for sr in 100
    do
      for p in 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0
       do
        schema_file_name="$root_data_path/$d/$d$sep$p.schema"
        sample_raw_fileName="$root_data_path/$d/sample_$sr$sep$p.raw"
        sample_frame_file_name="$root_data_path/$d/sample_$sr$sep$p.frame"
        delimiter=""
        if [[ "$d" == "imdb" ]]; then
          delimiter="\t";
        fi
        SCRIPT="$BASE_SCRIPT\
                $sample_raw_fileName\
                $sample_frame_file_name\
                $sr\
                $delimiter\
                $schema_file_name\
                $data_file_name\
                $p\
                $d\
                $home_log/benchmark/GIONestedExperiment/$d.csv
        "
        #echo $SCRIPT
        echo 3 > /proc/sys/vm/drop_caches && sync
        sleep 20
        time $SCRIPT
      done
    done
done
