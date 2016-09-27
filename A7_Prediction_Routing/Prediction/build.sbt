//Author: Harshali Singh

name              := "Prediction"
version           := "0.1.0-SNAPSHOT"
organization      := "com.github.harshali"
mainClass in (Compile) := Some("Prediction")
publishMavenStyle := true
crossPaths        := false
autoScalaLibrary  := false

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
   "org.apache.hadoop" % "hadoop-annotations" % "2.6.0",
   "org.apache.hadoop" % "hadoop-common" % "2.6.0",
   "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0",
   "commons-cli" % "commons-cli" % "1.2",
   "nz.ac.waikato.cms.weka" % "weka-dev" % "3.7.5"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}
