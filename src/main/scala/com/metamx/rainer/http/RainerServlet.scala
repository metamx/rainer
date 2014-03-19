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

import com.google.common.base.Charsets
import com.google.common.io.ByteStreams
import com.metamx.common.scala.exception._
import com.metamx.common.scala.timekeeper.{SystemTimekeeper, Timekeeper}
import com.metamx.common.scala.{Logging, Jackson}
import com.metamx.rainer.{KeyValueDeserialization, Commit, CommitStorage}
import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import org.joda.time.DateTime
import org.scalatra.{Ok, NotFound, BadRequest, ScalatraServlet}

trait RainerServlet[ValueType] extends ScalatraServlet with Logging
{
  def commitStorage: CommitStorage[ValueType]

  def root: String = "/"

  def valueDeserialization: KeyValueDeserialization[ValueType]

  val timekeeper: Timekeeper = new SystemTimekeeper

  get(root) {
    json {
      for {
        (k, commit) <- commitStorage.heads
        value <- commit.value.flatMap(_.right.toOption)
      } yield {
        (k, commit.metadata)
      }
    }
  }

  get(root.stripSuffix("/") + "/:key/:version") {
    doGet(commitStorage.get(params("key"), params("version").toInt))
  }

  get(root.stripSuffix("/") + "/:key/:version/meta") {
    doGetMeta(commitStorage.get(params("key"), params("version").toInt))
  }

  get(root.stripSuffix("/") + "/:key") {
    doGet(commitStorage.get(params("key")))
  }

  get(root.stripSuffix("/") + "/:key/meta") {
    doGetMeta(commitStorage.get(params("key")))
  }

  post(root.stripSuffix("/") + "/:key/:version") {
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
      (request.header("X-Rainer-Author"), request.header("X-Rainer-Comment")) match {
        case (None, _) => BadRequest("Missing header: X-Rainer-Author")
        case (_, None) => BadRequest("Missing header: X-Rainer-Comment")
        case (Some(author), Some(comment)) =>
          val empty = request.header("X-Rainer-Empty").map(_.toLowerCase).getOrElse("false") match {
            case "yes" | "true" | "1" =>
              true
            case "no" | "false" | "0" =>
              false
            case _ =>
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
          val commit = Commit.create[ValueType](
            key,
            version,
            payload,
            author,
            comment,
            mtime
          )(valueDeserialization)
          commitStorage.save(commit).catchEither[IllegalArgumentException] match {
            case Right(()) =>
              Ok(json(commit.metadata))

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

  private def doGet(commitOption: Option[Commit[ValueType]]) = {
    commitOption match {
      case Some(commit) if commit.payload.isDefined =>
        Ok(
          textIfPossible(commit.payload.get),
          RainerServlet.commitHttpHeaders(commit)
        )
      case _ =>
        NotFound("Key not found")
    }
  }

  private def doGetMeta(commitOption: Option[Commit[ValueType]]) = {
    commitOption match {
      case Some(commit) =>
        json(commit.metadata)
      case None =>
        NotFound("Key not found")
    }
  }

  private def text(x: String) = {
    contentType = "text/plain; charset=UTF-8"
    x.getBytes(Charsets.UTF_8)
  }

  private def binary(x: Array[Byte]) = {
    contentType = "application/octet-stream"
    x
  }

  private def textIfPossible(x: Array[Byte]) = {
    val decoder = Charsets.UTF_8.newDecoder()
    decoder.onMalformedInput(CodingErrorAction.REPORT)
    try {
      text(decoder.decode(ByteBuffer.wrap(x)).toString)
    } catch {
      case e: CharacterCodingException =>
        binary(x)
    }
  }

  private def json[A](x: A) = {
    contentType = "application/json"
    Jackson.generate(x) + "\r\n"
  }
}

object RainerServlet
{
  /**
   * HTTP headers corresponding to a commit's metadata. These will be returned on GETs, and can be set on POSTs.
   * @return
   */
  def commitHttpHeaders[A](commit: Commit[A]): Map[String, String] = {
    Map(
      "X-Rainer-Key" -> commit.key,
      "X-Rainer-Version" -> commit.version.toString,
      "X-Rainer-Author" -> commit.author,
      "X-Rainer-Comment" -> commit.comment,
      "X-Rainer-Modified-Time" -> commit.mtime.toString(),
      "X-Rainer-Empty" -> (if (commit.payload.isEmpty) "Yes" else "No")
    )
  }
}
