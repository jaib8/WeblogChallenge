name := "WebPaytm"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.1.0",
"org.apache.spark" %% "spark-sql" % "2.1.0",
"joda-time" % "joda-time" % "2.9.4",
"org.apache.hadoop" % "hadoop-aws" % "2.7.1",
"com.amazonaws" % "aws-java-sdk" % "1.7.4"
)
