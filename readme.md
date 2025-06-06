# Important Commands

## File Upload Commands

```bash
# Upload JAR file to EC2 instance
scp -i cloud-project.pem tweet_classifier-1.0-SNAPSHOT.jar ubuntu@16.16.211.160:/home/ubuntu

# Upload CSV file to EC2 instance
scp -i tokyo_2020_tweets.csv ubuntu@16.16.211.160:/home/ubuntu
```

## Docker Commands

```bash
# Copy files into the namenode container
docker cp tweet_classifier-1.0-SNAPSHOT.jar namenode:/tmp/
docker cp tokyo_2020_tweets.csv namenode:/tmp/

# Access container shell
docker exec -it namenode /bin/bash
```

## HDFS Commands

```bash
# Create input directory
hdfs dfs -mkdir /user/root/input

# Copy file to HDFS
hdfs dfs -cp tokyo_2020_tweets.csv /user/root/input
```

## Hadoop Execution

```bash
# Navigate to JAR location
cd /tmp

# Run Hadoop job
hadoop jar tweet_classifier-1.0-SNAPSHOT.jar org.tweetClassifier.KeywordCounterDriver -D csv.text.column.index=10 input2 output
```

## Dataset Reference

Dataset source: [Kaggle - Cyber Security Attacks](https://www.kaggle.com/datasets/teamincribo/cyber-security-attacks/data)
