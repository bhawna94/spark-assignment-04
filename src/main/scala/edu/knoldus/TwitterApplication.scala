package edu.knoldus

import java.sql.DriverManager

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TwitterApplication extends App {
  /**
    * Constant values
    */
  val log = Logger.getLogger(this.getClass)
  val one = 1
  val three = 3
  val five = 5
  val driver = "com.mysql.jdbc.Driver"
  val user = "root"
  val password = "root"
  val url = "jdbc:mysql://localhost/tweets"
  Class.forName("com.mysql.jdbc.Driver")
  val connection = DriverManager.getConnection(url,user,password)
  val stmt = connection.createStatement
  /**
    * application.config
    */
  val config: Config = ConfigFactory.load("application.config")
  val consumerKey = config.getString("keydetails.details.ConsumerKey")
  val consumerSecret = config.getString("keydetails.details.ConsumerSecret")
  val accessToken = config.getString("keydetails.details.accessToken")
  val accessTokenSecret = config.getString("keydetails.details.accessTokenSecret")
  val spark = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Spark-streaming-with-twitter")
  val sparkcontext = new SparkContext(spark)
  val streamingcontext = new StreamingContext(sparkcontext, Seconds(five))
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().setDebugEnabled(true)
    .setOAuthConsumerKey(consumerKey)
    .setOAuthConsumerSecret(consumerSecret)
    .setOAuthAccessToken(accessToken)
    .setOAuthAccessTokenSecret(accessTokenSecret).build()))
  /**
    * Streaming of tweets
    */
  val twitterStream = TwitterUtils.createStream(streamingcontext,auth )
  val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
  val topCounts3: DStream[(Int, String)] = hashTagStream.map((_, one)).reduceByKeyAndWindow(_ + _, Seconds(five))
    .map { case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))
  /**
    * saving In database
    */
  topCounts3.foreachRDD(rdd => {
    val topList = rdd.take(three)
    log.info("Popular tweets (%s total):".format(rdd.count()))
    topList.foreach { case (count, tag) => log.info("%s (%s tweets)".format(tag, count))
      val query = " insert into toptweets (topics, count)" + " values (?, ?)"
      val preparedStmt = connection.prepareStatement(query)
      preparedStmt.setString(1, tag)
      preparedStmt.setInt(2, count)
      preparedStmt.execute
    }
    })
  streamingcontext.start()
  streamingcontext.awaitTermination()
}

