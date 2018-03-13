name := "spark-ml-practice"

version := "0.1"

scalaVersion := "2.11.11"

sparkVersion := "2.2.1"

retrieveManaged := true

sparkComponents ++= Seq("sql", "streaming", "mllib")

libraryDependencies ++= Seq(
  "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "mrpowers" % "spark-daria" % "2.2.0_0.12.0" % "test",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "com.typesafe" % "config" % "1.2.1",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}
