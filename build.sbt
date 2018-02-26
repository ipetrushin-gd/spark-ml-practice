name := "spark-ml-practice"

version := "0.1"

scalaVersion := "2.11.11"

sparkVersion := "2.2.1"

retrieveManaged := true

sparkComponents ++= Seq("sql", "streaming")

libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "mrpowers" % "spark-daria" % "2.2.0_0.12.0" % "test"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

 artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}
libraryDependencies += "com.typesafe" % "config" % "1.2.1"