/*
 * Rainer.
 * Copyright 2014 Metamarkets Group Inc.
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

package com.metamx.rainer

import com.metamx.common.scala.db.{DBConfig, DB}
import org.scala_tools.time.Imports._

abstract class DerbyMemoryDB(name: String) extends DB(DerbyMemoryDB.config(name)) with DbCommitStorageMixin
{
  override def canLimit = false

  override def createIsTransient = (t: Throwable) => false

  override def createTable(table: String, decls: Seq[String]) {
    execute("create table %s (%s)" format (table, decls mkString ", "))
  }
}

object DerbyMemoryDB
{
  def config(name: String) = new DBConfig {
    override def queryTimeout = 30.seconds

    override def password = ""

    override def batchSize = 100

    override def user = ""

    override def uri = "jdbc:derby:memory:%s;create=true" format name

    override def fetchSize = 100
  }
}

trait DerbyCommitTable
{
  self: DbCommitStorageMixin =>

  override def rainerTableSchema = Seq(
    "name          varchar(255)",
    "version       int",
    "payload       varchar(1024) for bit data",
    "is_empty      int",
    "primary key (name, version)"
  )
}

trait DerbyCommitTableWithoutIsEmpty
{
  self: DbCommitStorageMixin =>

  override def rainerTableSchema = Seq(
    "name          varchar(255)",
    "version       int",
    "payload       varchar(1024) for bit data",
    "primary key (name, version)"
  )
}
