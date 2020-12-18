#!/usr/bin/env bash

HOME=$(pwd)

functions=(
           "crawler/trigger_glue_crawler" \
           "crawler/get_glue_crawler_state" \
           "redshift/consumption" \
           "upload_result"  \
           "split_batch"  \
           "prepare_training"  \
           "prepare_batch"  \
           "meter_forecast" \
           "get_anomaly"  \
           "batch_anomaly_detection" \
           "state_topic_subscription" \
           "load_pipeline_parameter" \
           "outage_info" \
           "check_initial_pipeline_run")

for lambda_folder in ${functions[*]};
do
   function_name=${lambda_folder////_}
   echo $function_name
   (cd $lambda_folder; zip -9qr "$HOME/packages/${function_name}.zip" .;cd $HOME)
done