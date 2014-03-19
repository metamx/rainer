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

import com.metamx.common.scala.Logging
import com.metamx.common.scala.db.DB
import com.metamx.common.scala.untyped._

/**
 * CommitStorage implementation backed by an RDBMS.
 */
class DbCommitStorage[ValueType : KeyValueDeserialization](db: DB with DbCommitStorageMixin, tableName: String)
  extends CommitStorage[ValueType] with Logging
{
  private def commitFromMap(m: Dict): Commit[ValueType] = {
    Commit.deserializeOrThrow(m("payload").asInstanceOf[Array[Byte]])
  }

  def start() {
    if (!db.schema.exists(tableName)) {
      log.info("Creating table[%s].", tableName)
      db.createTable(tableName, db.rainerTableSchema)
    }
  }

  def stop() {
    // Nothing to do.
  }

  override def save(commit: Commit[ValueType]) {
    val commitBytes = Commit.serialize(commit)
    db.inTransaction {
      // Capture current max(version)
      val maxVersionSql = """select max(version) from :table: where name = ?""".replace(":table:", tableName)
      val maxVersion = db.select(maxVersionSql, commit.key).head.values.head match {
        case null => 0L
        case x => x.asInstanceOf[Int]
      }
      // User-supplied version must be max(version) + 1
      if (commit.version != maxVersion + 1) {
        throw new IllegalArgumentException(
          "Concurrent modification: %s: requested version (%s) was not next available version (%s)".format(
            commit.key, commit.version, maxVersion + 1
          )
        )
      }
      // Attempt to insert the new commit. If it fails, this means someone else beat us to it.
      db.execute(
        "insert into :table: (name, version, payload) values (?,?,?)".replace(":table:", tableName),
        commit.key,
        commit.version,
        commitBytes
      )
      commit
    }
  }

  override def get(key: Commit.Key) = {
    // LIMIT 1 not required for correctness, so only include it if we can.
    val sql =
      """select payload from :table:
         where name = ? order by version desc %s""".format(if (db.canLimit) "limit 1" else "")
    db.select(sql.replace(":table:", tableName), key).headOption.map(commitFromMap)
  }

  override def get(key: Commit.Key, version: Int) = {
    val sql = """select payload from :table: where name = ? and version = ?"""
    db.select(sql.replace(":table:", tableName), key, version).headOption.map(commitFromMap)
  }

  override def keys = {
    val sql = """select distinct :table:.name from :table:"""
    db.select(sql.replace(":table:", tableName)).map(row => str(row("name")))
  }

  override def heads = {
    val sql =
      """
      select :table:.name, :table:.payload
      from :table:
           join (select name, max(version) as version from :table: group by name) as :table:_max on :table:.name = :table:_max.name and :table:.version = :table:_max.version
      """
    db.select(sql.replace(":table:", tableName)).map(row => str(row("name")) -> commitFromMap(row)).toMap
  }
}

trait DbCommitStorageMixin
{
  self: DB =>

  def rainerTableSchema: Seq[String]

  def canLimit = true
}

trait DbCommitStorageMySQLMixin extends DbCommitStorageMixin
{
  self: DB =>

  override def rainerTableSchema = Seq(
    "name          varbinary(255)  not null",
    "version       int             not null",
    "payload       mediumblob      not null", // Max size 16MB
    "primary key (name, version)"
  )
}
