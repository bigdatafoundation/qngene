name := "optithera-importer"

version := "0.2"

scalaVersion := "2.11.8"

//latest versions from https://mvnrepository.com/
libraryDependencies ++= Seq(
  "com.opencsv" % "opencsv" % "3.8",
  "org.apache.avro" % "avro" % "1.8.1",
  "com.twitter" % "parquet-avro" % "1.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" % "spark-core_2.11" % "1.6.2",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.2"
)
