package tech.sourced

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


package object queryset {

  private[queryset] def identifyLicense: UserDefinedFunction = {
    udf[String, Array[Byte]](rawContent => {
      val content = rawContent.map(_.toChar).mkString
      val matchedLicenses = Licenses.filter(license => {
        content.contains(license.matcher) && content.contains(license.version)
      })

      if (matchedLicenses.isEmpty) {
        "Unknown License"
      } else {
        matchedLicenses.head.id
      }
    })
  }

  private[this] case class License(id: String, matcher: String, version: String)

  //see https://spdx.org/licenses/
  // Example list of licenses
  private[this] val Licenses: Seq[License] = Seq(
    License("AGPL-3.0", "GNU AFFERO GENERAL PUBLIC LICENSE", "Version 3"),
    License("GPL-1.0", "GNU GENERAL PUBLIC LICENSE", "Version 1"),
    License("GPL-2.0", "GNU GENERAL PUBLIC LICENSE", "Version 2"),
    License("GPL-3.0", "GNU GENERAL PUBLIC LICENSE", "Version 3"),
    License("LGPL-2.1", "GNU LESSER GENERAL PUBLIC LICENSE", "Version 2.1"),
    License("LGPL-3.0", "GNU LESSER GENERAL PUBLIC LICENSE", "Version 3.0"),
    License("MPL-1.0", "Mozilla Public License Version 1.0", ""),
    License("MPL-1.1", "Mozilla Public License Version 1.1", ""),
    License("MPL-2.0", "Mozilla Public License Version 2.0", ""),
    License(
      "Apache-1.0", "Copyright (c) 1995-1999 The Apache Group. All rights reserved.", ""),
    License("Apache-1.1", "Apache License 1.1", ""),
    License("Apache-2.0", "Apache License", "Version 2.0"),
    License("MIT", "MIT License", ""),
    License("ZPL-1.1", "Zope Public License", "Version 1.1"),
    License("ZPL-2.0", "Zope Public License", "Version 2.0"),
    License("ZPL-2.1", "Zope Public License", "Version 2.1"),
    License(
      "The Unlicense", "This is free and unencumbered software released into the public domain.",
      "")
  )

  private[queryset] def lookForDbs: UserDefinedFunction = {
    udf[Array[String], Array[Byte]](rawContent => {
      val content = rawContent.map(_.toChar).mkString
      val dbList = DbMap.keys.filter(dbName =>
        DbMap(dbName).exists(content.contains(_))
      ).toArray

      dbList
    })
  }

  // Example list of databases
  private[this] val DbMap: Map[String, Seq[String]] = Map(
    "MySql" -> Seq("MySql", "mysql", "MYSQL"),
    "PostgreSQL" -> Seq("Postgre", "postgre", "POSTGRE"),
    "SQLite" -> Seq("SQLite", "sqlite", "SQLITE"),
    "MongoDB" -> Seq("Mongo, mongo, MONGO"),
    "Cassandra" -> Seq("Cassandra, cassandra, CASSANDRA"),
    "Oracle" -> Seq("Oracle", "oracle", "ORACLE")
  )

}
