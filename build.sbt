name := "naqiran-spark-xml"

version := "0.1"

scalaVersion := "2.12.0"

resolvers += "Maven Central" at "http://central.maven.org/maven2/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"