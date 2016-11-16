name := """spark-demo"""

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "1.6.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-mllib" % "1.6.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-hive" % "1.6.0" withSources() withJavadoc())

