import com.github.catalystcode.fortis.spark.streaming.rss._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}


case class TrainingTweet(id: Long, label: Integer, text: String)

object RSSDemo {
  def main(args: Array[String]) {
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")
    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)
    val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
    import spark.sqlContext.implicits._
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tweets = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("train.csv").as[TrainingTweet]
      .withColumn("text", functions.lower(functions.col("text")))
//      .select("ItemId", "SentimentText", "Sentiment")
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(10000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))
    tweets.printSchema()
    val model = pipeline.fit(tweets)
    stream.foreachRDD(rdd => {
      val columnName = "title"
      val tweets = rdd.toDS()
        .select("uri", "title")
        .withColumn(columnName, functions.lower(functions.col(columnName)))
        .withColumn("id", functions.monotonically_increasing_id())
        .withColumn("text", functions.col("title"))
      val result = model.transform(tweets).select("uri", "probability", "prediction")
      result.show()
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
