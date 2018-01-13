<<<<<<< HEAD
name := "spark-devp-learning"
=======
name := "SparkDemo"
>>>>>>> 4307a02c4711b9a71360d1ea053b6eb2c3d9c217

version := "1.0"

scalaVersion := "2.11.8"

val gtVersion = "1.0.0-SNAPSHOT"

libraryDependencies ++=  Seq(
  "org.apache.spark"     %    "spark-core_2.11"     %     "2.0.0",
  "org.apache.spark"     %    "spark-sql_2.11"      %     "2.0.0",
  "org.apache.spark"     %    "spark-streaming_2.11"    %  "2.0.0",
  "org.apache.spark"     %    "spark-mllib_2.11"     %     "2.0.0",
  "org.apache.spark"     %    "spark-hive_2.11"     %     "2.0.0",
  "com.typesafe.akka"    %    "akka-actor_2.11"     %     "2.4.4",
  "org.eclipse.jetty"  % "jetty-client" % "8.1.14.v20131031",
  "net.sf.opencsv" % "opencsv" % "2.0"
)

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/DUMMY.SF" => MergeStrategy.discard
  case "META-INF/DUMMY.RSA" => MergeStrategy.discard
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}