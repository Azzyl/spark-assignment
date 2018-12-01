import com.github.catalystcode.fortis.spark.streaming.rss._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Normalizer, Tokenizer}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}


case class TrainingTweet(tweetId: Long, label: Integer, tweetContent: String)

object RSSDemo {

  val durationSeconds = 60 //update time

  var conf :SparkConf = _
  var sc  :SparkContext = _
  var ssc :StreamingContext = _
  var urlCSV :String = _ //our RSS link

  def init(): Unit ={
    conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    sc = new SparkContext(conf)
    ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")
  }
  def setRssUrl (url :String): Unit ={
    urlCSV = url
  }
  def getUrls(): Array[String] ={
    urlCSV.split(",")
  }
  def RSSToRDD(rssUrl :Array[String]): RSSInputDStream ={
    new RSSInputDStream(rssUrl, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, connectTimeout = 60000, pollingPeriodInSeconds = durationSeconds)

  }

  def getLogisticRegression(): LogisticRegression ={
      new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.01)
  }

  def getSVM(): LinearSVC ={
    new LinearSVC()
      .setMaxIter(10)
  }

  def normalizeData(): Unit ={

  }

  def main(args: Array[String]) {

    init()
    urlCSV = "https://queryfeed.net/twitter?q=putin&title-type=user-name-both&order-by=recent&geocode="
    if (args.length > 0) setRssUrl(args(0))

    val urls = getUrls() //get url of each twit

    val tweetsFromRSS = RSSToRDD(urls) // RSS TO RDD

    val spark = SparkSession.builder().appName(sc.appName).getOrCreate() //create spark
    import spark.sqlContext.implicits._
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)


    var trainTweets = sqlContext.read.format("com.databricks.spark.csv") //train tweets for model to learn
      .option("header", "true")
      .option("inferSchema", "true")
      .load("train.csv").as[TrainingTweet] //adding train.csv
      .withColumn("tweetContent", functions.lower(functions.col("tweetContent")))


    val tokenizer = new Tokenizer()
      .setInputCol("tweetContent")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setNumFeatures(10000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")

    val regression1 = getSVM() //
    val regression2 = getLogisticRegression()

    val pipeline1 = new Pipeline()
      .setStages(
        Array(
          tokenizer,   // 1) tokenize
          hashingTF,   // 2) hashing
          regression1)) // 3) making regression

    val pipeline2 = new Pipeline()
      .setStages(
        Array(
          tokenizer,   // 1) tokenize
          hashingTF,   // 2) hashing
          regression2)) // 3) making regression


    trainTweets = trainTweets
                .withColumn("tweetContent", functions.regexp_replace(
                     functions.col("tweetContent"),
                """[\p{Punct}&&[^.]]""", ""))

    val Array(trainingData, testData) = trainTweets.randomSplit(Array(0.7, 0.3))

    //fitting model with preprocessing data
    val model1 = pipeline1.fit(trainingData)
    val predictions1 = model1.transform(testData)

    val model2 = pipeline2.fit(trainingData)
    val predictions2 = model2.transform(testData)


    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy1 = evaluator.evaluate(predictions1)
    val accuracy2 = evaluator.evaluate(predictions2)
    var finalModel = model2
    if(accuracy1<accuracy2) {finalModel = model1}

    println("Test Error in SVM = " + (1.0 - accuracy1))
    println("Test Error in Logistic Regression = " + (1.0 - accuracy2))

    tweetsFromRSS.foreachRDD(rdd => { //creating stream to get tweets
      if (!rdd.isEmpty()) {

        //from RDD to DS
        var tweets = rdd.toDS()
          .select("uri", "title")
          .withColumn("title", functions.lower(functions.col("title")))
          .withColumn("tweetId", functions.monotonically_increasing_id())
          .withColumn("tweetContent", functions.col("title"))

        tweets = tweets.withColumn("tweetContent",
          functions.regexp_replace(functions.col("tweetContent"),
            """[\p{Punct}&&[^.]]""", ""))

        //predict for DS
        val predictions = finalModel.transform(tweets)
          .select("title", "prediction", "probability")

        //printing result
        print(predictions.show() + " hello")


        //save results to file
        predictions.toDF().write.mode(SaveMode.Append).save("output")
      }
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
