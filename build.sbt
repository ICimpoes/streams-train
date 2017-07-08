name := "streams-train"

version := "1.0"

scalaVersion := "2.12.2"

val akkaVersion = "2.5.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test
)