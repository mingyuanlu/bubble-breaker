#!/bin/bash

case $1 in

   --new)
      export TAX_LIST_FILE=$PWD/data/list-of-tax.csv
      export THEME_LIST_FILE=$PWD/data/list-of-top-themes.csv
      export SRC_LIST_FILE=$PWD/data/list-of-top-src.csv
      nohup sh $PWD/src/spark/run-generate-tax.sh get-taxonomy-list.py 10 $TAX_LIST_FILE
      nohup sh $PWD/src/spark/run-generate-list.sh get-top-themes.py $THEME_LIST_FILE
      nohup sh $PWD/src/spark/run-generate-list.sh get-src-list.py $SRC_LIST_FILE
      nohup sh $PWD/src/spark/run.sh compute-analytics-top-cleaned-themes-daily-with-src.py &
      ;;

   --existing)
      export TAX_LIST_FILE=$PWD/data/list-of-tax-2018.csv
      export THEME_LIST_FILE=$PWD/data/list-of-top-themes-2015to9-500.csv
      export SRC_LIST_FILE=$PWD/data/list-of-top-src-2018.csv
      nohup sh $PWD/src/spark/run.sh compute-analytics-top-cleaned-themes-daily-with-src.py &
      ;;

   *)

      echo "Usage: ./run.sh [--new|--existing]"
      ;;

esac


