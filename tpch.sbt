name := "Spark TPC-H Queries"

version := "1.0"

scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "csdstorage",
    libraryDependencies := Seq(
      "org.apache.spark" %% "spark-core" % "3.3.0",
      "org.apache.spark" %% "spark-sql" % "3.3.0"
    )
  )
