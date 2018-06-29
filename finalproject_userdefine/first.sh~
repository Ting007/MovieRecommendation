#!/bin/sh
rm ./finalOutput
rm ./middleresult/part-r-00000
rm ./middleresult/wikiscore
hadoop fs -rm -r finalproject
hadoop fs -mkdir finalproject
hadoop fs -copyFromLocal descrptionInput finalproject
hadoop jar wikiuserdescription.jar finalproject/descrptionInput finalproject/descrptionOutput -file configuration/conf
hadoop fs -copyToLocal finalproject/descrptionOutput/part-r-00000 middleresult/part-r-00000
sort -n -r -k2 middleresult/part-r-00000 >>middleresult/wikiscore
hadoop jar IDtoContent.jar finalproject/descrptionInput finalproject/finalOutput -file middleresult/wikiscore
hadoop fs -copyToLocal finalproject/finalOutput/part-r-00000 finalOutput


