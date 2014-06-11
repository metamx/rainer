organization := "com.metamx"

name := "rainer"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1", "2.10.4")

lazy val root = project.in(file("."))

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= Seq(
  "Metamarkets Releases" at "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local/"
)

publishMavenStyle := true

publishTo := Some("pub-libs" at "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local")

parallelExecution in Test := false

testOptions += Tests.Argument(TestFrameworks.JUnit, "-Duser.timezone=UTC")

releaseSettings

val curatorVersion = "2.3.0"

def ScalatraCross = CrossVersion.binaryMapped {
  case "2.9.1" => "2.9.2"
  case x => x
}

libraryDependencies ++= Seq(
  "com.metamx" %% "scala-util" % "1.8.9",
  "org.slf4j" % "slf4j-api" % "1.6.5",
  "javax.servlet" % "javax.servlet-api" % "3.0.1",
  "org.eclipse.jetty" % "jetty-servlet" % "8.1.10.v20130312",
  "com.google.guava" % "guava" % "14.0.1",
  "org.scalatra" % "scalatra" % "2.2.1" cross ScalatraCross
)

libraryDependencies ++= Seq(
  "org.apache.curator" % "curator-framework" % curatorVersion exclude("org.jboss.netty", "netty"),
  "org.apache.curator" % "curator-recipes" % curatorVersion exclude("org.jboss.netty", "netty"),
  "org.apache.curator" % "curator-x-discovery" % curatorVersion exclude("org.jboss.netty", "netty")
)

libraryDependencies <+= scalaVersion {
  case "2.9.1" => "com.simple" % "simplespec_2.9.2" % "0.7.0" % "test"
  case "2.10.4" => "com.simple" % "simplespec_2.10.2" % "0.8.4" % "test"
}

libraryDependencies ++= Seq(
  "org.apache.derby" % "derby" % "10.10.1.1" % "test",
  "org.apache.curator" % "curator-test" % curatorVersion % "test",
  "org.scalatra" % "scalatra-test" % "2.2.1" % "test" cross ScalatraCross
)

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.10" % "test"
)
