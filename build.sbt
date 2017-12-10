import sbt.Keys.dependencyOverrides
import sbt._

organization := "org.apache.spark"

name := "spark-runner"

version in ThisBuild := "1.1.0"

val assemblyName = "spark-runner-assembly"

scalaVersion in ThisBuild := "2.11.12"

scalastyleFailOnError := true

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture"
)

wartremoverErrors ++= Warts.all

val sparkVersion = "2.2.0.cloudera1"

val hadoopVersion = "2.6.0-cdh5.13.0"

val zookeeperVersion = "3.4.5-cdh5.13.0"

val scalaTestVersion = "3.0.1"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

val isALibrary = true //this is a library project

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("org.apache.hadoop", "hadoop-yarn-client").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")

val assemblyDependencies = (scope: String) => Seq(
  sparkExcludes("org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % scope),
  "org.apache.zookeeper" % "zookeeper" % zookeeperVersion % scope
)

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api")

/*if it's a library the scope is "compile" since we want the transitive dependencies on the library
  otherwise we set up the scope to "provided" because those dependencies will be assembled in the "assembly"*/
lazy val assemblyDependenciesScope: String = if (isALibrary) "compile" else "provided"

lazy val hadoopDependenciesScope = if (isALibrary) "provided" else "compile"

libraryDependencies ++= Seq(
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % hadoopDependenciesScope),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client" % hadoopVersion % hadoopDependenciesScope)
) ++ assemblyDependencies(assemblyDependenciesScope)

dependencyOverrides in ThisBuild += "org.apache.zookeeper" % "zookeeper" % zookeeperVersion
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-base" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-json-provider" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.module" % "jackson-module-jaxb-annotations" % "2.6.5"
dependencyOverrides in ThisBuild += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.5"

//Trick to make Intellij/IDEA happy
//We set all provided dependencies to none, so that they are included in the classpath of root module
lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
  // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
  libraryDependencies := (libraryDependencies in RootProject(file("."))).value.map {
    module =>
      if (module.configurations.equals(Some("provided"))) {
        module.copy(configurations = None)
      } else {
        module
      }
  }
)

//http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

//http://stackoverflow.com/questions/27824281/sparksql-missingrequirementerror-when-registering-table
fork := false

parallelExecution in Test := false

isSnapshot := true

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(Defaults.itSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17" % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )
  ).
  settings(
    organizationName := "David Greco",
    startYear := Some(2017),
    licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))
  ).
  settings(
    mappings in(Compile, packageBin) ~= {
      _.filter(!_._1.getName.endsWith("log4j.properties"))
    }
  ).
  enablePlugins(AutomateHeaderPlugin).
  enablePlugins(JavaAppPackaging).
  disablePlugins(AssemblyPlugin)

lazy val projectAssembly = (project in file("assembly")).
  settings(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyMergeStrategy in assembly := {
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case PathList("log4j.properties") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    assemblyJarName in assembly := s"$assemblyName-${version.value}.jar",
    libraryDependencies ++= assemblyDependencies("compile")
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (projectID in root).value.excludeAll(ExclusionRule(organization = "org.apache.spark"),
        if (!isALibrary) ExclusionRule(organization = "org.apache.hadoop") else ExclusionRule())
    )
  })

mappings in Universal := {
  val universalMappings = (mappings in Universal).value
  val filtered = universalMappings filter {
    case (f, n) =>
      !n.endsWith(s"${organization.value}.${name.value}-${version.value}.jar")
  }
  val fatJar: File = new File(s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/$assemblyName-${version.value}.jar")
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}

scriptClasspath ++= Seq(s"$assemblyName-${version.value}.jar")
