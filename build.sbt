ThisBuild / organization := "com.pcpitman"
ThisBuild / version := {
  import scala.sys.process._
  def gitSilent(cmd: String): Option[String] = {
    val out = new StringBuilder
    val logger = ProcessLogger(s => out.append(s), _ => ())
    val exitCode = cmd.!(logger)
    if (exitCode == 0) Some(out.toString.trim) else None
  }
  def stripInit(v: String): String =
    if (v.endsWith("-init")) v.dropRight(5) else v
  def incrementPatch(v: String): String = {
    val parts = v.split('.')
    if (parts.length == 3) s"${parts(0)}.${parts(1)}.${parts(2).toInt + 1}"
    else v
  }
  val branch = "git rev-parse --abbrev-ref HEAD".!!.trim
  val commitTag = gitSilent("git describe --tags --exact-match HEAD")
  val lastTag = gitSilent("git describe --tags --abbrev=0")
  val commit = "git rev-parse HEAD".!!.trim.take(8)
  val timestamp = {
    val fmt = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    fmt.format(new java.util.Date())
  }
  def snapshot(base: String): String =
    s"$base-$branch-$timestamp-$commit-SNAPSHOT"

  commitTag match {
    case Some(tag) if tag.endsWith("-init") =>
      snapshot(stripInit(tag))
    case Some(tag) if branch == "main" =>
      tag
    case Some(tag) =>
      snapshot(tag)
    case None =>
      lastTag match {
        case Some(tag) if tag.endsWith("-init") =>
          snapshot(stripInit(tag))
        case Some(tag) =>
          snapshot(incrementPatch(tag))
        case None =>
          snapshot("0.0.0")
      }
  }
}

lazy val root = (project in file("."))
  .aggregate(dynamodb, kms, ses, sns, s3, redis)
  .settings(
    name := "mock-clients",
    publish / skip := true
  )

val noSnapshotDeps = taskKey[Unit]("Fail if release build has SNAPSHOT dependencies")

lazy val commonSettings = Seq(
  scalaVersion := "3.8.1",
  crossScalaVersions := Seq("2.13.18", "3.8.1"),
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature") ++
    (if (scalaVersion.value.startsWith("3")) Seq("-Wconf:msg=vararg splices:s") else Seq.empty),
  libraryDependencies ++= Seq(
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.log4jApi       % Test,
    Dependencies.log4jCore      % Test,
    Dependencies.log4jSlf4j     % Test,
    Dependencies.munit          % Test
  ),
  testFrameworks += new TestFramework("munit.Framework"),
  publishMavenStyle := true,
  publishTo := Some("GitHub Packages" at "https://maven.pkg.github.com/bpitman/mock-clients"),
  credentials += Credentials(
    "GitHub Package Registry",
    "maven.pkg.github.com",
    sys.env.getOrElse("GITHUB_ACTOR", ""),
    sys.env.getOrElse("GITHUB_TOKEN", "")
  ),
  noSnapshotDeps := {
    if (!isSnapshot.value) {
      val snapshots = libraryDependencies.value.filter(_.revision.endsWith("-SNAPSHOT"))
      if (snapshots.nonEmpty) {
        val desc = snapshots.map(d => s"  ${d.organization}:${d.name}:${d.revision}").mkString("\n")
        sys.error(s"Release build ${version.value} has SNAPSHOT dependencies:\n$desc")
      }
    }
  },
  publish := (publish dependsOn noSnapshotDeps).value,
  publishLocal := (publishLocal dependsOn noSnapshotDeps).value
)

lazy val dynamodb = (project in file("dynamodb"))
  .settings(commonSettings)
  .settings(
    name := "mock-clients-dynamodb",
    libraryDependencies ++= Seq(
      Dependencies.aws2DynamoDB
    )
  )

lazy val kms = (project in file("kms"))
  .settings(commonSettings)
  .settings(
    name := "mock-clients-kms",
    libraryDependencies ++= Seq(
      Dependencies.aws2KMS
    )
  )

lazy val ses = (project in file("ses"))
  .settings(commonSettings)
  .settings(
    name := "mock-clients-ses",
    libraryDependencies ++= Seq(
      Dependencies.aws2SES
    )
  )

lazy val sns = (project in file("sns"))
  .settings(commonSettings)
  .settings(
    name := "mock-clients-sns",
    libraryDependencies ++= Seq(
      Dependencies.aws2SNS
    )
  )

lazy val s3 = (project in file("s3"))
  .settings(commonSettings)
  .settings(
    name := "mock-clients-s3",
    libraryDependencies ++= Seq(
      Dependencies.aws2S3
    )
  )

lazy val redis = (project in file("redis"))
  .settings(commonSettings)
  .settings(
    name := "mock-clients-redis",
    libraryDependencies ++= Seq(
      Dependencies.lettuce
    )
  )
