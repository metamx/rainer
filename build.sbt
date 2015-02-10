organization := "com.metamx"

name := "rainer"

scalaVersion := "2.9.1"

crossScalaVersions := Seq("2.9.1", "2.10.4")

lazy val root = project.in(file("."))

net.virtualvoid.sbt.graph.Plugin.graphSettings

licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

homepage := Some(url("https://github.com/metamx/rainer"))

publishMavenStyle := true

publishTo := Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/")

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>https://github.com/metamx/rainer.git</url>
    <connection>scm:git:git@github.com:metamx/rainer.git</connection>
  </scm>
  <developers>
    <developer>
      <name>Gian Merlino</name>
      <organization>Metamarkets Group Inc.</organization>
      <organizationUrl>https://www.metamarkets.com</organizationUrl>
    </developer>
  </developers>)

parallelExecution in Test := false

testOptions += Tests.Argument(TestFrameworks.JUnit, "-Duser.timezone=UTC")

releaseSettings

ReleaseKeys.publishArtifactsAction := PgpKeys.publishSigned.value

val curatorVersion = "2.3.0"

def ScalatraCross = CrossVersion.binaryMapped {
  case "2.9.1" => "2.9.2"
  case x => x
}

libraryDependencies ++= Seq(
  "com.metamx" %% "scala-util" % "1.9.0",
  "javax.servlet" % "javax.servlet-api" % "3.0.1",
  "org.eclipse.jetty" % "jetty-servlet" % "8.1.10.v20130312",
  "com.google.guava" % "guava" % "15.0",
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
