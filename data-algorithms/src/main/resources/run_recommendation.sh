#!/bin/bash
export SPARK_HOME=/home/ubuntu/spark
APP_JAR=/home/ubuntu/data-practise/data-algorithms/target/data-algorithms-1.0-SNAPSHOT-jar-with-dependencies.jar
MOVIES=/home/ubuntu/data/movieRecommendation.txt
$SPARK_HOME/bin/spark-submit --class com.spark.recommendation.MovieRecommendationWithJoin --master yarn  --deploy-mode cluster $APP_JAR $MOVIES