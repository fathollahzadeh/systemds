#!/usr/bin/env bash

#SystemDS Paths:
#----------------------------------------------------------------
systemDS_Home="/home/saeed/Documents/Github/systemds"
LOG4JPROP="$systemDS_Home/scripts/perftest/conf/log4j.properties"
jar_file_path="$systemDS_Home/target/SystemDS.jar"
lib_files_path="$systemDS_Home/target/lib/*"
#-----------------------------------------------------------------
root_data_path="/home/saeed/Documents/Dataset/GIODataset/json"
home_log="/home/saeed/Documents/ExpLog/json/"
cpp_base_src="" #"/home/sfathollahzadeh/Documents/GitHub/papers/2022-icde-gIO/experiments/benchmark/RapidJSONCPP/src/at/tugraz"
sep="_"
nrows=-1

mx_mem="$(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / (1024 * 1024 * 1024)))g"

delimiter="\t"
declare -a  datasets=("aminer_paper")
declare -a  main_classes=("GIOFrameExperimentHDFS") #SYSDSFrameExperimentHDFS GIOFrameExperimentHDFS

for (( i = 0; i < 1; i++ )); do
      for mc in "${main_classes[@]}"; do
        for d in "${datasets[@]}"; do
          ./resultPath.sh $home_log $d$i $mc
          data_file_name="$root_data_path/$d/$d.data"

          for sr in 1000 #200 300 400 500 600 700 800 900 1000
          do
            for p in 11 #0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1.0
              do
                  #schema_file_name="$root_data_path/$d/$d.schema"
                  #sample_raw_fileName="$root_data_path/$d/sample_$sr$sep$p.raw"
                  #sample_frame_file_name="$root_data_path/$d/sample_$sr$sep$p.frame"

                  schema_file_name="$root_data_path/$d/$d$sep$p.schema"
                  sample_raw_fileName="$root_data_path/$d/sample_$sr$sep$p.raw"
                  sample_frame_file_name="$root_data_path/$d/sample_$sr$sep$p.frame"
                  SCRIPT="java\
                  -Dlog4j.configuration=file:$LOG4JPROP\
                  -Xms1g\
                  -Xmx$mx_mem\
                  -Xmn4000m\
                  -DsampleRawFileName=$sample_raw_fileName\
                  -DsampleFrameFileName=$sample_frame_file_name\
                  -DsampleNRows=$sr\
                  -Ddelimiter=$delimiter\
                  -DschemaFileName=$schema_file_name\
                  -DdataFileName=$data_file_name\
                  -DdatasetName=$d\
                  -DhomeLog=$home_log/benchmark/$mc/$d$i.csv\
                  -DcppBaseSrc=$cpp_base_src\
                  -Dnrows=$nrows\
                  -cp\
                  $jar_file_path:$lib_files_path\
                  org.apache.sysds.runtime.iogen.exp.$mc\
                  "
                #echo 3 > /proc/sys/vm/drop_caches && sync
                #sleep 20
                
                #echo "++++++++++++++++++++++++++++++++++++++++++++"
                #echo $SCRIPT
                time $SCRIPT
              done
          done
        done
      done
done
#/home/saeed/Documents/Dataset/GIODataset/flat/aminer_paper
#/home/saeed/Documents/GIODataset/flat/aminer_paper/aminer_paper_5.schema
