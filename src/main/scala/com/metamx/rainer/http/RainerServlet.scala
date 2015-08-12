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

package com.metamx.rainer.http

import com.google.common.io.ByteStreams
import com.metamx.common.scala.Logging
import com.metamx.common.scala.exception._
import com.metamx.common.scala.timekeeper.{SystemTimekeeper, Timekeeper}
import com.metamx.rainer.{CommitOrderingException, CommitMetadata, KeyValueDeserialization, Commit, CommitStorage}
import org.joda.time.DateTime
import org.scalatra.{Conflict, Ok, BadRequest, ScalatraServlet}

trait RainerServlet[ValueType] extends ScalatraServlet with RainerServletBase with Logging
{
  def commitStorage: CommitStorage[ValueType]
  def valueDeserialization: KeyValueDeserialization[ValueType]

  val timekeeper: Timekeeper = new SystemTimekeeper

  get("/") {
    val heads = if (shouldListAll.getOrElse(false)) commitStorage.heads else commitStorage.headsNonEmpty
    doList(heads)
  }

  get("/:key/:version") {
    doGet(commitStorage.get(params("key"), params("version").toInt))
  }

  get("/:key/:version/meta") {
    doGetMeta(commitStorage.get(params("key"), params("version").toInt))
  }

  get("/:key") {
    doGet(commitStorage.get(params("key")))
  }

  get("/:key/meta") {
    doGetMeta(commitStorage.get(params("key")))
  }

  post("/:key/:version") {
    class ClientException(msg: String) extends Exception(msg)
    try {
      val key = params("key")
      val version = params("version").toInt
      val bytes = ByteStreams.toByteArray(request.inputStream)
      if (request.header("X-Rainer-Key").exists(_ != key)) {
        throw new ClientException("X-Rainer-Key must match URL key, or be omitted")
      }
      if (request.header("X-Rainer-Version").exists(_ != version.toString)) {
        throw new ClientException("X-Rainer-Version must match URL version, or be omitted")
      }
      request.header("X-Rainer-Author") match {
        case None => BadRequest("Missing header: X-Rainer-Author")

        case Some(author) =>
          val comment = request.header("X-Rainer-Comment").getOrElse("")

          val empty = RainerServlet.yesNo(request.header("X-Rainer-Empty").getOrElse("false")) getOrElse {
            throw new ClientException("Malformed header: X-Rainer-Empty")
          }
          val mtime = request.header("X-Rainer-Modified-Time").map(new DateTime(_)).getOrElse(timekeeper.now)
          val payload = if (empty) {
            if (bytes.nonEmpty) {
              throw new ClientException("Empty payload required with X-Rainer-Empty")
            }
            None
          } else {
            valueDeserialization.fromKeyAndBytes(params("key"), bytes).catchEither[Exception] match {
              case Right(value) =>
                Some(bytes)

              case Left(e) =>
                throw new ClientException("Malformed input: %s" format e.toString)
            }
          }
          val commit = Commit[ValueType](
            key,
            version,
            payload,
            author,
            comment,
            mtime
          )(valueDeserialization)
          commitStorage.save(commit).catchEither[Exception] match {
            case Right(()) =>
              Ok(json(commit.meta.asMap))

            case Left(e: CommitOrderingException) =>
              Conflict(json(Map(
                "conflict" -> Map(
                  "key" -> e.key,
                  "expectedVersion" -> e.expectedVersion,
                  "providedVersion" -> e.providedVersion
                )
              )))

            case Left(e: IllegalArgumentException) =>
              // This is likely a user error.
              BadRequest(e.getMessage)
          }
      }
    } catch {
      case e: ClientException =>
        BadRequest(e.getMessage)
    }
  }
}

object RainerServlet
{
  def yesNo(s: String): Option[Boolean] = Some(s.toLowerCase) collect {
    case "yes" | "true" | "1" =>
      true
    case "no" | "false" | "0" =>
      false
  }

  /**
   * HTTP headers corresponding to a commit's metadata. These will be returned on GETs, and can be set on POSTs.
   */
  def headersForCommitMetadata(meta: CommitMetadata): Map[String, String] = {
    Map(
      "X-Rainer-Key" -> meta.key,
      "X-Rainer-Version" -> meta.version.toString,
      "X-Rainer-Author" -> meta.author,
      "X-Rainer-Comment" -> meta.comment,
      "X-Rainer-Modified-Time" -> meta.mtime.toString(),
      "X-Rainer-Empty" -> (if (meta.empty) "Yes" else "No")
    )
  }

  /**
   * Commit metadata corresponding to HTTP headers (the reverse of `headersForCommit`):
   */
  def commitMetadataForHeaders(headers: Map[String, String]): CommitMetadata = {
    CommitMetadata(
      headers("X-Rainer-Key"),
      headers("X-Rainer-Version").toInt,
      headers("X-Rainer-Author"),
      headers("X-Rainer-Comment"),
      new DateTime(headers("X-Rainer-Modified-Time")),
      yesNo(headers("X-Rainer-Empty")).getOrElse(false)
    )
  }
}
