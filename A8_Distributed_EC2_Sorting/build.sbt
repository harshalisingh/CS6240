//Author: Harshali Singh

name              := "Node"
version           := "0.1.0-SNAPSHOT"
organization      := "com.github.harshali"
mainClass in (Compile) := Some("Node")
publishMavenStyle := true
crossPaths        := false
autoScalaLibrary  := false

// library dependencies. (orginization name) % (project name) % (version)
libraryDependencies ++= Seq(
	"com.amazonaws" % "aws-java-sdk" % "1.10.65",
	"commons-io" % "commons-io" % "2.4"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
 case entry => {
   val strategy = mergeStrategy(entry)
   if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
   else strategy
 }
}}

