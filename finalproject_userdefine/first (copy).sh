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

hadoop fs -copyToLocal finalproject/input1/movies.csv step1



hadoop fs -copyFromLocal words.text test/input/words.txt

hadoop jar wikiandmovielenID.jar final/intput/ final/output/ 

hadoop fs -get /test/part-r-00000 /step1

java -jar run.jar -s 1

hadoop jar wikiIDtoContent final/intput/ final/output/ 

java -jar run.jar -s 2





sort -n -r -k2 ./part-r-00000 >>IDfile



