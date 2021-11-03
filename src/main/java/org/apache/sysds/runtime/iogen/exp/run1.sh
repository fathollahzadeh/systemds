#!/usr/bin/env bash

#rm -rf results
LOG4JPROP='/home/sfathollahzadeh/Documents/GitHub/systemds/log4j.properties'
mkdir -p results
DATA_HOME="/home/sfathollahzadeh/GRTest/Datasets/Dataset1"
runs=1
nrows=1000
ncols=1000
sparsity=1.0
sep="_"
nr="nrows"
nc="ncols"
sp="sparsity"
file_name="$sep$nr$sep$nrows$sep$nc$sep$ncols$sep$sp$sep$sparsity.raw"

calc(){ awk "BEGIN { print "$*" }"; }

declare -a  datasets1=("CSV"
                      "LIBSVM-FZ"
                      "LIBSVM-FO"
                      "MM-FZ"
                      "MM-FO"
                      "MM-FZ-SYM-UT"
                      "MM-FZ-SYM-LT"
                      "MM-FO-SYM-UT"
                      "MM-FO-SYM-LT"
                      "MM-FZ-SKEW-UT"
                      "MM-FZ-SKEW-LT"
                      "MM-FO-SKEW-UT"
                      "MM-FO-SKEW-LT"
                      )
declare -a  datasets=("CSV")

for d in "${datasets[@]}"; do
  out_log="results/synthetic-$d.csv"
  echo "dataset,sparsity,data_nrows,data_ncols,raw_nrows,mapping_time,analysis_time,read_time,total_read_time" > $out_log

  for (( rc = 100; rc <= 100; rc +=10 )); do
      rlen=$rc
      #############################################
      sum_mapping_time=0
      sum_analysis_time=0
      sum_read_time=0
      sum_total_read_time=0

      for (( r = 0; r < $runs; r++ )); do
        data="$DATA_HOME/data/$d$file_name"
        clen=$ncols

        dn=$d
        if [[ "$d" == "LIBSVM"* ]]; then
            clen=$(($ncols+1))
            dn="LIBSVM"
         elif [[ "$d" == "MM-FZ-SYM"* || "$d" == "MM-FO-SYM"* ]]; then
            dn="MM-SYM"
       elif [[ "$d" == "MM-FZ-SKEW"* || "$d" == "MM-FO-SKEW"* ]]; then
            dn="MM-SKEW"
       elif [[ "$d" == "MM-FZ" || "$d" == "MM-FO" ]]; then
            dn="MM"
        fi

        sample_raw="$DATA_HOME/samples/$d$sep$nr$sep$rc$sep$nc$sep$rc$sep$sp$sep$sparsity.raw"
        sample="$DATA_HOME/samples/$dn$sep$nr$sep$rc$sep$nc$sep$rc$sep$sp$sep$sparsity.matrix"
        args="$data $sample_raw $sample $clen $rlen $rlen"
        echo $args
        java -cp /home/sfathollahzadeh/Documents/GitHub/systemds/target/SystemDS.jar org.apache.sysds.iogenexp.matrix $args $LOG4JPROP
#
#        result_log="results/out-$r-$HOSTNAME-"`date +"%d-%m-%Y"`".log"
#
#        mapping_time="$(grep -F 'mapping_time' $result_log)"
#        arrIN=(${mapping_time//:/ })
#        mapping_time=${arrIN[1]}
#
#        analysis_time="$(grep -F 'analysis_time' $result_log)"
#        arrIN=(${analysis_time//:/ })
#        analysis_time=${arrIN[1]}
#
#        read_time="$(grep -F 'read_time' $result_log)"
#        arrIN=(${read_time//:/ })
#        read_time=${arrIN[1]}
#
#        total_read_time="$(grep -F 'total_read_time' $result_log)"
#        arrIN=(${total_read_time//:/ })
#        total_read_time=${arrIN[1]}
#
#        sum_mapping_time=$(calc $sum_mapping_time+$mapping_time)
#        sum_analysis_time=$(calc $analysis_time+$sum_analysis_time)
#        sum_read_time=$(calc $read_time+$sum_read_time)
#        sum_total_read_time=$(calc $total_read_time+$sum_total_read_time)
#
#        raw_nrows="$(grep -F 'raw_nrows' $result_log)"
#        arrIN=(${raw_nrows//:/ })
#        raw_nrows=${arrIN[1]}
#
#        rm -rf $result_log
      done

#      avg_mapping_time=$(calc $sum_mapping_time/$runs)
#      avg_analysis_time=$(calc $sum_analysis_time/$runs)
#      avg_read_time=$(calc $sum_read_time/$runs)
#      avg_total_read_time=$(calc $sum_total_read_time/$runs)
#      echo "$d,$sparsity,$nrows,$ncols,$raw_nrows,$avg_mapping_time,$avg_analysis_time,$avg_read_time,$avg_total_read_time" >> $out_log
#      echo "Run: $r  nrows=$rlen"
      ########################################################
    done
done














#!/usr/bin/env bash

#mvn package -P distribution
rm -rf results
mkdir -p results
DATA_HOME="/home/sfathollahzadeh/GRTest/Datasets/Dataset1"
runs=1
nrows=100
ncols=100
sparsity=1.0
sep="_"
nr="nrows"
nc="ncols"
sp="sparsity"
file_name="$sep$nr$sep$nrows$sep$nc$sep$ncols$sep$sp$sep$sparsity"

calc(){ awk "BEGIN { print "$*" }"; }

declare -a  datasets1=("CSV"
                      "LIBSVM-FZ"
                      "LIBSVM-FO"
                      "MM-FZ"
                      "MM-FO"
                      "MM-FZ-SYM-UT"
                      "MM-FZ-SYM-LT"
                      "MM-FO-SYM-UT"
                      "MM-FO-SYM-LT"
                      "MM-FZ-SKEW-UT"
                      "MM-FZ-SKEW-LT"
                      "MM-FO-SKEW-UT"
                      "MM-FO-SKEW-LT"
                      )

declare -a  datasets=("CSV")
#declare -a  datasets=("MM-FZ-SYM-UT")

for d in "${datasets[@]}"; do
  out_log="results/synthetic-$d.log"
  echo "dataset,sparsity,data_nrows,data_ncols,raw_nrows,mapping_time,analysis_time,read_time,total_read_time" > $out_log

  for (( rc = 10; rc <= 15; rc +=10 )); do
      rlen=$rc
      sample_rlen=$rc

      case $d in
        "MM-FZ" | "MM-FO") rlen=$(calc rc*$ncols);;
        "MM-FZ-SYM-UT" | "MM-FZ-SYM-LT" | "MM-FO-SYM-UT" | "MM-FO-SYM-LT") rlen=$(calc $(($rc*$(($rc+1))))/2);;
        "MM-FZ-SKEW-UT" | "MM-FZ-SKEW-LT" | "MM-FO-SKEW-UT" | "MM-FO-SKEW-LT") rlen=$(calc $(($rc*$(($rc-1))))/2);;
      esac
      echo "$d >>>  $rlen"

      #############################################
      sum_mapping_time=0
      sum_analysis_time=0
      sum_read_time=0
      sum_total_read_time=0

      for (( r = 0; r < $runs; r++ )); do
        data="$DATA_HOME/$d$file_name.txt"
        clen=$ncols
        dn="sampleMatrix"
        if [[ "$d" == *"LIBSVM"* ]]; then
            clen=$(($ncols+1))
            dn="sampleMatrixLIBSVM"
        elif [[ "$d" == *"MM-FZ-SYM"* ]]; then
            dn="sampleMatrixMMSYM"
        elif [[ "$d" == *"MM-FZ-SKEW"* ]]; then
            dn="sampleMatrixMMSKEW"
        fi

        sample="$DATA_HOME/samples/$dn$file_name.matrix"
        sample_raw="$DATA_HOME/samples/$dn$file_name.raw"
        args="$data $sample_raw $sample $clen $rlen $rlen"
        java -cp /home/sfathollahzadeh/Documents/GitHub/systemds/target/SystemDS.jar org.apache.sysds.iogenexp.matrix $args
        echo $args
        result_log="results/out-$r-$HOSTNAME-"`date +"%d-%m-%Y"`".log"

      #/home/sfathollahzadeh/GRTest/Datasets/Dataset1/samples

#        mapping_time="$(grep -F 'mapping_time' $result_log)"
#        arrIN=(${mapping_time//:/ })
#        mapping_time=${arrIN[1]}
#
#        analysis_time="$(grep -F 'analysis_time' $result_log)"
#        arrIN=(${analysis_time//:/ })
#        analysis_time=${arrIN[1]}
#
#        read_time="$(grep -F 'read_time' $result_log)"
#        arrIN=(${read_time//:/ })
#        read_time=${arrIN[1]}
#
#        total_read_time="$(grep -F 'total_read_time' $result_log)"
#        arrIN=(${total_read_time//:/ })
#        total_read_time=${arrIN[1]}
#
#        sum_mapping_time=$(calc $sum_mapping_time+$mapping_time)
#        sum_analysis_time=$(calc $analysis_time+$sum_analysis_time)
#        sum_read_time=$(calc $read_time+$sum_read_time)
#        sum_total_read_time=$(calc $total_read_time+$sum_total_read_time)

#        raw_nrows="$(grep -F 'raw_nrows' $result_log)"
#        arrIN=(${raw_nrows//:/ })
#        raw_nrows=${arrIN[1]}

        #rm -rf $result_log
      done

#      avg_mapping_time=$(calc $sum_mapping_time/$runs)
#      avg_analysis_time=$(calc $sum_analysis_time/$runs)
#      avg_read_time=$(calc $sum_read_time/$runs)
#      avg_total_read_time=$(calc $sum_total_read_time/$runs)
#      echo "$d,$sparsity,$nrows,$ncols,$rlen,$avg_mapping_time,$avg_analysis_time,$avg_read_time,$avg_total_read_time" >> $out_log
      echo "Run: $r  nrows=$rlen"
      ########################################################
    done
done




