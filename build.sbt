libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.apache.hadoop" % "hadoop-test" % "1.0.4" % Test,
  "org.hamcrest" % "hamcrest-all" % "1.1" % Test,
  "org.apache.mrunit" % "mrunit" % "1.1.0" % Test classifier "hadoop1",
  "org.apache.hadoop" % "hadoop-core" % "1.0.4",
  "org.apache.hbase" % "hbase" % "0.94.6",
  "com.google.guava" % "guava" % "18.0",
  "joda-time" % "joda-time" % "2.0"
)

lazy val root = (project in file(".")).
  settings(
    name := "opentsdb-mr",
    version := "0.1",
    scalaVersion := "2.11.6",
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      artifact.name + "." + artifact.extension
    },
    distr := {
      (Keys.`package` in Compile).value
      sbt.IO.delete(libDir.value)
      val depFiles = update.value.matching((s: String) => Set("compile", "runtime") contains s)
      for (depFile <- depFiles) {
        sbt.IO.copyFile(depFile, libDir.value/ depFile.name, true)
      }
      var jarPath=(artifactPath in (Compile, packageBin)).value
      sbt.IO.copyFile(jarPath, libDir.value / ("../" + jarPath.name))
    }
  )

lazy val distr = taskKey[Unit]("Task to copy dependencies jars")

lazy val libDir = settingKey[File]("the directory to retrieve dependencies")

libDir := baseDirectory.value / "bin/libs"