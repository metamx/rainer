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
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.db.DB
import com.metamx.common.scala.exception._
import com.metamx.common.scala.untyped._
import org.skife.jdbi.v2.exceptions.StatementException

/**
 * CommitStorage implementation backed by an RDBMS.
 */
class DbCommitStorage[ValueType : KeyValueDeserialization](db: DB with DbCommitStorageMixin, tableName: String)
  extends CommitStorage[ValueType] with Logging
{
  // Does this table have the is_empty field? (It's optional)
  private lazy val tableHasIsEmpty = !raises[StatementException] {
    db.select("select is_empty from %s where name = ?" format tableName, "dummy")
  } withEffect {
    b =>
      if (!b) {
        log.warn("The table[%s] does not have an is_empty column. Excluding it from queries.", tableName)
      }
  }

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
      if (tableHasIsEmpty) {
        db.execute(
          "insert into :table: (name, version, payload, is_empty) values (?,?,?,?)".replace(":table:", tableName),
          commit.key,
          commit.version,
          commitBytes,
          if (commit.isEmpty) 1 else 0
        )
      } else {
        db.execute(
          "insert into :table: (name, version, payload) values (?,?,?)".replace(":table:", tableName),
          commit.key,
          commit.version,
          commitBytes
        )
      }
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

  override def heads = {
    getHeads(nonEmpty = false)
  }

  override def headsNonEmpty = {
    // nonEmpty is a hint rather than a hard-and-fast thing. So we still need to filter.
    getHeads(nonEmpty = true) filter (!_._2.isEmpty)
  }

  private def getHeads(nonEmpty: Boolean) = {
    val sql =
      """
      select :table:.name, :table:.payload
      from :table:
           join (select name, max(version) as version from :table: group by name) as :table:_max on :table:.name = :table:_max.name and :table:.version = :table:_max.version
      :where:
      """
    val where = if (nonEmpty && tableHasIsEmpty) "where :table:.is_empty != 1" else ""
    db.select(sql.replace(":where:", where).replace(":table:", tableName))
      .map(row => str(row("name")) -> commitFromMap(row))
      .toMap
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
    "is_empty      tinyint         not null", // Boolean. If true, this commit is definitely empty.
    "primary key (name, version)",
    "key (is_empty)"
  )
}
