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

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.untyped.{str, dict, int, Dict}
import com.metamx.rainer.http.RainerServlet
import com.twitter.finagle.Service
import com.twitter.util.{Future, Await}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpVersion, DefaultHttpRequest, HttpResponse, HttpRequest}
import scala.collection.JavaConverters._

/**
 * CommitStorage implementation backed by a remote rainer servlet.
 *
 * @param client Finagle service pointing at the remote server.
 * @param baseUri URI of the service at the remote server. Will not actually be used to make a connection (the
 *                `client` is used for that) but will be used to set the "Host" header and determine service paths.
 */
class HttpCommitStorage[ValueType: KeyValueDeserialization](
  client: Service[HttpRequest, HttpResponse],
  baseUri: URI
) extends CommitStorage[ValueType]
{
  override def start() {}

  override def stop() {}

  override def heads = {
    val theRequest = request(HttpMethod.GET, "", Some("all=yes&payload_base64=yes"))
    val theResponse = client(theRequest).map(failOnErrors).map(asHeads)
    Await.result(theResponse)
  }

  override def headsNonEmpty = {
    val theRequest = request(HttpMethod.GET, "", Some("payload_base64=yes"))
    val theResponse = client(theRequest).map(failOnErrors).map(asHeads)
    Await.result(theResponse)
  }

  override def get(key: Commit.Key, version: Int) = Await.result(asyncGet(key, Some(version)))

  override def get(key: Commit.Key) = Await.result(asyncGet(key, None))

  override def save(commit: Commit[ValueType]) {
    val response =
      client(
        request(HttpMethod.POST, "/%s/%d" format(commit.key, commit.version), None) withEffect {
          req =>
            val bytes = commit.payload.getOrElse(Array.empty[Byte])
            for ((k, v) <- RainerServlet.headersForCommitMetadata(commit.meta)) {
              req.headers().set(k, v)
            }
            req.headers().set("Content-Length", bytes.size)
            req.headers().set("Content-Type", "application/octet-stream")
            req.setContent(ChannelBuffers.wrappedBuffer(bytes))
        }
      )
    Await.result(response.map(throwOrderingExceptions).map(failOnErrors))
  }

  private def uri(suffix: String, queryString: Option[String]): URI = {
    baseUri.withPath(_ + suffix).withQuery(queryString.orNull)
  }

  private def request(method: HttpMethod, pathSuffix: String, queryString: Option[String]) = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, uri(pathSuffix, queryString).toString) withEffect {
      req =>
        val hostAndPort = baseUri.host + (if (baseUri.port > 0) {
          ":%d" format baseUri.port
        } else {
          ""
        })
        req.headers().set("Host", hostAndPort)
        req.headers().set("Accept", "*/*")
    }
  }

  private def asyncGet(key: Commit.Key, version: Option[Int]): Future[Option[Commit[ValueType]]] = {
    val path = version match {
      case Some(v) => "/%s/%d" format(key, v)
      case None => "/%s" format key
    }
    val commit = client(request(HttpMethod.GET, path, None))
      .map(swallow404)
      .map(_.map(failOnErrors))
      .map(_.map(asCommit))
    commit
  }

  /**
   * Convert 404s into Nones, pass through as Somes otherwise.
   */
  private def swallow404(response: HttpResponse): Option[HttpResponse] = {
    if (response.getStatus.getCode != 404) {
      Some(response)
    } else {
      None
    }
  }

  /**
   * Throw an exception if the response status is non-200, pass through otherwise.
   */
  private def failOnErrors(response: HttpResponse): HttpResponse = {
    if (response.getStatus.getCode / 100 == 2) {
      response
    } else {
      throw new IllegalArgumentException(
        "HTTP request failed: %d %s: %s" format
          (response.getStatus.getCode, response.getStatus.getReasonPhrase, response.getContent.toString(Charsets.UTF_8))
      )
    }
  }

  /**
   * Detect and throw CommitOrderingExceptions if present; otherwise don't change the response.
   */
  private def throwOrderingExceptions(response: HttpResponse) = {
    // Maybe throw an exception.
    if (response.getStatus.getCode == 409) {
      val orderingException: Option[CommitOrderingException] = try {
        val d = Jackson.parse[Dict](response.getContent.toString(Charsets.UTF_8))
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
  private def asBytes(response: HttpResponse): Array[Byte] = {
    val byteBuffer = response.getContent.toByteBuffer
    val bytes = Array.ofDim[Byte](byteBuffer.remaining())
    byteBuffer.get(bytes)
    bytes
  }

  /**
   * Extract Commit object from the response.
   */
  private def asCommit(response: HttpResponse): Commit[ValueType] = {
    val meta = RainerServlet.commitMetadataForHeaders(
      response.headers().asScala.map {
        entry =>
          (entry.getKey, entry.getValue)
      }.toMap
    )
    val bytes = asBytes(response)
    Commit(meta, if (meta.empty) None else Some(bytes))
  }

  /**
   * Extract Map of Commits from the response.
   */
  private def asHeads(response: HttpResponse): Map[Commit.Key, Commit[ValueType]] = {
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
  private def parseJson[A: ClassManifest](response: HttpResponse): A = {
    Jackson.parse[A](asBytes(response))
  }
}
