organization := "com.metamx"

name := "rainer"

scalaVersion := "2.11.11"

crossScalaVersions := Seq("2.11.11", "2.12.2")

lazy val root = project.in(file("."))

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

releasePublishArtifactsAction := PgpKeys.publishSigned.value

// Target Java 8
scalacOptions += "-target:jvm-1.8"
javacOptions in compile ++= Seq("-source", "1.8", "-target", "1.8")

val curatorVersion = "2.11.1"

libraryDependencies ++= Seq(
  "com.metamx" %% "scala-util" % "1.13.4",
  "javax.servlet" % "javax.servlet-api" % "3.1.0",
  "org.eclipse.jetty" % "jetty-servlet" % "9.4.5.v20170502",
  "com.google.guava" % "guava" % "16.0.1"
)

libraryDependencies ++= Seq(
  "org.apache.curator" % "curator-framework" % curatorVersion exclude("org.jboss.netty", "netty"),
  "org.apache.curator" % "curator-recipes" % curatorVersion exclude("org.jboss.netty", "netty"),
  "org.apache.curator" % "curator-x-discovery" % curatorVersion exclude("org.jboss.netty", "netty"),
  "org.scalatra" %% "scalatra" % "2.5.0" exclude("com.typesafe.akka", "akka-actor"),
  "org.scalatra" %% "scalatra-test" % "2.5.0" % "test" exclude("com.typesafe.akka", "akka-actor") exclude("org.mockito", "mockito-all") force()
)

// Test stuff
libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % "test" force(),
  "org.mockito" % "mockito-core" % "1.9.5" % "test" force(),
  "org.apache.derby" % "derby" % "10.10.1.1" % "test",
  "org.apache.curator" % "curator-test" % curatorVersion % "test",
  "ch.qos.logback" % "logback-core" % "1.1.2" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.2" % "test",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25" % "test",
  "org.slf4j" % "jul-to-slf4j" % "1.7.25" % "test"
)

libraryDependencies <++= scalaVersion {
  case x if x.startsWith("2.11.") => Seq(
    "com.simple" %% "simplespec" % "0.8.4" % "test" exclude("org.mockito", "mockito-all") force()
  )
  case _ => Seq(
    "com.simple" %% "simplespec" % "0.9.0" % "test" exclude("org.mockito", "mockito-all") force()
  )
}

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11-RC1" % "test" exclude("junit", "junit") force()
)