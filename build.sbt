name := "bd-spark"

version := "0.1"

scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  //...
  "com.github.catalystcode" %% "streaming-rss-html" % "1.0.2",
  "org.apache.spark" %% "spark-core" % "2.2.0"
  //...
)