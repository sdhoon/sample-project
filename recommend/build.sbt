name := "Recommend1"

version := "1.0"

scalaVersion := "2.10.6"
libraryDependencies += "org.jblas" % "jblas" % "1.2.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.2"
libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5")
