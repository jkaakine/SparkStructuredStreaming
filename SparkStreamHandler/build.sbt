name := "IoT data Stream Handler"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
	"org.apache.spark" %% "spark-sql" % "3.1.1" % "provided"
)



libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.1.1"