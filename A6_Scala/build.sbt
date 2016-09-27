//Author: Harshali Singh

lazy val root = (project in file(".")).
    settings(
        name := "Missed Connection",
	version := "1.0",
	mainClass in Compile := Some("MissedConnection"),
        libraryDependencies ++= Seq(
		"org.apache.spark" %% "spark-core" % "1.5.2",
		"joda-time" % "joda-time" % "2.9.2"
	))

