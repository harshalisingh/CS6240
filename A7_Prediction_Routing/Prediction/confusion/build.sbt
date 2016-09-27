//Author: Harshali Singh

lazy val root = (project in file(".")).
    settings(
        name := "Confusion",
	version := "1.0",
	mainClass in Compile := Some("Confusion"),
        libraryDependencies ++= Seq(
		"org.apache.spark" %% "spark-core" % "1.5.2"))

