### IMPORTANT URLS

## Hadoop docker

[https://github.com/big-data-europe/docker-hadoop](https://github.com/big-data-europe/docker-hadoop)

## Video URL

[https://www.youtube.com/watch?v=dLTI2HN9Ejg&t=46s](https://www.youtube.com/watch?v=dLTI2HN9Ejg&t=46s)

### IMPORTANT COMMANDS

## Copy the file into the container namenode (Jar file in temp)

docker cp hadoop-mapreduce-examples-2.7.1-sources.jar namenode:/tmp/

## Go inside to the container bash

docker exec -it namenode /bin/bash

## Add the file into docker (name node)

docker cp sample.txt namenode:/tmp/

## Add the file into the hdfs file system

1.  Create a dir
    ```
    hdfs dfs -mkdir /user/root/input
    ```
2.  Copy file
    ```
    hdfs dfs -put input.txt /user/root/input/
    ```

## Go to the file that added to file system

hdfs dfs -ls /user/root/input

## Run the hadoop (Need to in the folder where jar file is located)

hadoop jar hadoop-mapreduce-examples-2.7.1-sources.jar org.apache.hadoop.examples.WordCount input output

hadoop jar simpleWordCount-1.0-SNAPSHOT.jar org.simpleWordCount.AttackFrequencyDriver input output

## FILE UPLOAD COMMANDS

scp -i cloud-project.pem hadoop-mapreduce-examples-2.7.1-sources.jar ubuntu@16.16.211.160:/home/ubuntu

## DATA SET

[https://www.kaggle.com/datasets/teamincribo/cyber-security-attacks/data](https://www.kaggle.com/datasets/teamincribo/cyber-security-attacks/data)
