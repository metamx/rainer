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

import com.google.common.io.BaseEncoding
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.untyped.{Dict, dict, int, str}
import com.metamx.rainer.http.RainerServlet
import com.twitter.finagle.Service
import com.twitter.finagle.http.{Method, Request, Response}
import com.twitter.io.Buf
import com.twitter.util.{Await, Future}

/**
 * CommitStorage implementation backed by a remote rainer servlet.
 *
 * @param client Finagle service pointing at the remote server.
 * @param baseUri URI of the service at the remote server. Will not actually be used to make a connection (the
 *                `client` is used for that) but will be used to set the "Host" header and determine service paths.
 */
class HttpCommitStorage[ValueType: KeyValueDeserialization](
  client: Service[Request, Response],
  baseUri: URI
) extends CommitStorage[ValueType]
{
  override def start() {}

  override def stop() {}

  override def heads = {
    val theRequest = request(Method.Get, "", Some("all=yes&payload_base64=yes"))
    val theResponse = client(theRequest).map(failOnErrors).map(asHeads)
    Await.result(theResponse)
  }

  override def headsNonEmpty = {
    val theRequest = request(Method.Get, "", Some("payload_base64=yes"))
    val theResponse = client(theRequest).map(failOnErrors).map(asHeads)
    Await.result(theResponse)
  }

  override def get(key: Commit.Key, version: Int) = Await.result(asyncGet(key, Some(version)))

  override def get(key: Commit.Key) = Await.result(asyncGet(key, None))

  override def save(commit: Commit[ValueType]) {
    val response =
      client(
        request(Method.Post, "/%s/%d" format(commit.key, commit.version), None) withEffect {
          req =>
            val bytes = commit.payload.getOrElse(Array.empty[Byte])
            for ((k, v) <- RainerServlet.headersForCommitMetadata(commit.meta)) {
              req.headerMap.set(k, v)
            }
            req.headerMap.set("Content-Length", bytes.size.toString)
            req.headerMap.set("Content-Type", "application/octet-stream")
            req.content =  Buf.ByteArray.Shared(bytes)
        }
      )
    Await.result(response.map(throwOrderingExceptions).map(failOnErrors))
  }

  private def uri(suffix: String, queryString: Option[String]): URI = {
    baseUri.withPath(_ + suffix).withQuery(queryString.orNull)
  }

  private def request(method: Method, pathSuffix: String, queryString: Option[String]) = {
    Request(method, uri(pathSuffix, queryString).toString) withEffect {
      req =>
        val hostAndPort = baseUri.host + (if (baseUri.port > 0) {
          ":%d" format baseUri.port
        } else {
          ""
        })
        req.headerMap.set("Host", hostAndPort)
        req.headerMap.set("Accept", "*/*")
    }
  }

  private def asyncGet(key: Commit.Key, version: Option[Int]): Future[Option[Commit[ValueType]]] = {
    val path = version match {
      case Some(v) => "/%s/%d" format(key, v)
      case None => "/%s" format key
    }
    val commit = client(request(Method.Get, path, None))
      .map(swallow404)
      .map(_.map(failOnErrors))
      .map(_.map(asCommit))
    commit
  }

  /**
   * Convert 404s into Nones, pass through as Somes otherwise.
   */
  private def swallow404(response: Response): Option[Response] = {
    if (response.statusCode != 404) {
      Some(response)
    } else {
      None
    }
  }

  /**
   * Throw an exception if the response status is non-200, pass through otherwise.
   */
  private def failOnErrors(response: Response): Response = {
    if (response.statusCode / 100 == 2) {
      response
    } else {
      throw new IllegalArgumentException(
        "HTTP request failed: %d %s: %s" format
          (response.statusCode, response.status.reason, response.contentString)
      )
    }
  }

  /**
   * Detect and throw CommitOrderingExceptions if present; otherwise don't change the response.
   */
  private def throwOrderingExceptions(response: Response) = {
    // Maybe throw an exception.
    if (response.statusCode == 409) {
      val orderingException: Option[CommitOrderingException] = try {
        val d = Jackson.parse[Dict](response.contentString)
        d.get("conflict").map(dict(_)) map {
          conflictDict =>
            new CommitOrderingException(
              str(conflictDict("key")),
              int(conflictDict("expectedVersion")),
              int(conflictDict("providedVersion"))
            )
        }
      } catch {
        case e: Exception =>
          // suppress
          None
      }
      orderingException foreach (throw _)
    }

    // Otherwise don't change the response.
    response
  }

  /**
   * Extract bytes from the response.
   */
  private def asBytes(response: Response): Array[Byte] = {
    val buf = response.content
    val bytes = Array.ofDim[Byte](buf.length)
    buf.write(bytes, 0)
    bytes
  }

  /**
   * Extract Commit object from the response.
   */
  private def asCommit(response: Response): Commit[ValueType] = {
    val meta = RainerServlet.commitMetadataForHeaders(response.headerMap.toMap)
    val bytes = asBytes(response)
    Commit(meta, if (meta.empty) None else Some(bytes))
  }

  /**
   * Extract Map of Commits from the response.
   */
  private def asHeads(response: Response): Map[Commit.Key, Commit[ValueType]] = {
    val d = parseJson[Dict](response)
    for ((k, data) <- d) yield {
      val meta = CommitMetadata.fromMap(dict(data))
      val payload = if (meta.empty) None else Some(BaseEncoding.base64().decode(str(dict(data)("payload_base64"))))
      (k, Commit(meta, payload))
    }
  }

  /**
   * Parse response content as JSON.
   */
  private def parseJson[A: ClassManifest](response: Response): A = {
    Jackson.parse[A](asBytes(response))
  }
}
