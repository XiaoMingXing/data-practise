#!/bin/bash
export SPARK_HOME=/home/ubuntu/spark
APP_JAR=/home/ubuntu/data-practise/data-algorithms/target/data-algorithms-1.0-SNAPSHOT-jar-with-dependencies.jar
MOVIES=/home/ubuntu/data/movieRecommendation.txt
spark-submit \
--class com.spark.recommendation.MovieRecommendationWithJoin \
--master yarn \
--deploy-mode cluster \
/home/ubuntu/data-practise/data-algorithms/target/data-algorithms-1.0-SNAPSHOT-jar-with-dependencies.jar \
/user/ubuntu/movies.txt
/user/ubuntu/output