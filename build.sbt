/*
 * Copyright 2021 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

organization := "org.apache.spark"

name := "spark-runner"

ThisBuild / version := "2.0.0"

val assemblyName = "spark-runner-assembly"

ThisBuild / scalaVersion := "2.13.5"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

javacOptions ++= Seq(
  "-Xlint:unchecked"
)

wartremoverErrors ++= Warts.all

val sparkVersion = "3.2.0"

val hadoopVersion = "3.1.1"

val scalaTestVersion = "3.2.10"

val isALibrary = true //this is a library project

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client-api").
    exclude("org.apache.hadoop", "hadoop-client-runtime")

val assemblyDependencies = (_: String) => Seq(
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
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client-api" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client-runtime" % hadoopVersion % hadoopDependenciesScope)

) ++ assemblyDependencies(assemblyDependenciesScope)

//Trick to make Intellij/IDEA happy
//We set all provided dependencies to none, so that they are included in the classpath of root module
lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).
  configs(IntegrationTest).
  settings(
    // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
    libraryDependencies := (RootProject(file(".")) / libraryDependencies).value.map {
      module =>
        if (module.configurations.contains("provided"))
          module.withConfigurations(None)
        else
          module
    }
  )

//http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath/21803413#21803413
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)

IntegrationTest / fork := true

IntegrationTest / parallelExecution := false

IntegrationTest / javaOptions ++= Seq("--add-opens", "java.base/jdk.internal.loader=ALL-UNNAMED")

lazy val root = (project in file(".")).
  configs(IntegrationTest).
  settings(
    Defaults.itSettings,
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17" % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test, it"
    )
  ).
  settings(
    organizationName := "David Greco",
    startYear := Some(2021),
    licenses += ("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt"))
  ).
  settings(
    Compile / packageBin / mappings ~= {
      _.filter(!_._1.getName.endsWith("log4j.properties"))
    }
  ).
  enablePlugins(AutomateHeaderPlugin).
  disablePlugins(AssemblyPlugin)

lazy val projectAssembly = (project in file("assembly")).
  settings(
    //assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assembly / assemblyMergeStrategy := {
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case PathList("log4j.properties") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / assemblyJarName := s"$assemblyName-${version.value}.jar",
    libraryDependencies ++= assemblyDependencies("compile")
  ) dependsOn root settings (
  projectDependencies := {
    Seq(
      (root / projectID).value.excludeAll(ExclusionRule(organization = "org.apache.spark"),
        if (!isALibrary) ExclusionRule(organization = "org.apache.hadoop") else ExclusionRule())
    )
  })