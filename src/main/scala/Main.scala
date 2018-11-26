import com.github.catalystcode.fortis.spark.streaming.rss._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression

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
    val lr = new LogisticRegression()
//    var model1 = lr.fit()
    stream.foreachRDD(rdd => {
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      val tweets = rdd.toDS()
      rdd.foreach(tweet => {
        println(tweet.uri)
        println(tweet.title)
        tweet.content.foreach(rssContent => println(rssContent.value))
        println(tweet.content.length)
      })
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}
