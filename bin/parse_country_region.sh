#!/bin/bash
. /home/data/bin/misc.sh
home_setup
tslogged
BASE_DIR=$(dirname $HOME)
set_logdir "$BASE_DIR/log"
LOC_FILE="$BASE_DIR/data/GeoLiteCity_latest/GeoLiteCity-Location.csv"
tail -n+3 $LOC_FILE | sed  's/"//g' | awk -F"," '{if ($4 == "") {print $2"\t"$3"\t"$6"\t"$7}}'
