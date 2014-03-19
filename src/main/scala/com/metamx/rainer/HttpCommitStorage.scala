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
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.untyped.{int, dict, Dict}
import com.metamx.rainer.http.RainerServlet
import com.twitter.finagle.Service
import com.twitter.util.{Future, Await}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpMethod, HttpVersion, DefaultHttpRequest, HttpResponse, HttpRequest}
import scala.collection.immutable.Iterable

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

  override def keys = {
    Await.result(
      client(request(HttpMethod.GET, "")).map(failOnErrors).map(parseJson[Dict]).map(_.keys)
    )
  }

  override def heads = {
    Await.result(
      client(request(HttpMethod.GET, "")).map(failOnErrors).map(parseJson[Dict]) flatMap {
        d =>
          val kvFutures: Iterable[Future[(Commit.Key, Commit[ValueType])]] = {
            for ((k, meta) <- d) yield {
              val version = int(dict(meta)("version"))
              val commitFuture = asyncGet(k, Some(version))
              commitFuture map {
                commitOption =>
                  val commit = commitOption getOrElse {
                    throw new IllegalStateException(
                      "Inconsistent state: Listed key[%s] version[%s] but could not get details." format
                        (k, version)
                    )
                  }
                  (k, commit)
              }
            }
          }
          Future.collect(kvFutures.toSeq).map(_.toMap)
      }
    )
  }

  override def get(key: Commit.Key, version: Int) = Await.result(asyncGet(key, Some(version)))

  override def get(key: Commit.Key) = Await.result(asyncGet(key, None))

  override def save(commit: Commit[ValueType]) {
    val response =
      client(
        request(HttpMethod.POST, "/%s/%d" format(commit.key, commit.version)) withEffect {
          req =>
            val bytes = commit.payload.getOrElse(Array.empty[Byte])
            for ((k, v) <- RainerServlet.commitHttpHeaders(commit)) {
              req.headers().set(k, v)
            }
            req.headers().set("Content-Length", bytes.size)
            req.headers().set("Content-Type", "application/octet-stream")
            req.setContent(ChannelBuffers.wrappedBuffer(bytes))
        }
      )
    Await.result(response.map(failOnErrors))
  }

  private def uri(suffix: String) = baseUri withPath (_ + suffix)

  private def request(method: HttpMethod, pathSuffix: String) = {
    new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, uri(pathSuffix).path) withEffect {
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
    val base = version match {
      case Some(v) => "/%s/%d" format(key, v)
      case None => "/%s" format key
    }
    val meta = client(request(HttpMethod.GET, base + "/meta"))
      .map(swallow404)
      .map(_.map(failOnErrors))
      .map(_.map(parseJson[Dict]))
    val payload = client(request(HttpMethod.GET, base))
      .map(swallow404)
      .map(_.map(failOnErrors))
      .map(_.map(asBytes))
    Future.join(meta, payload) map {
      case (Some(m), Some(p)) =>
        Some(Commit.fromMetadataAndPayload(m, p))
      case (None, None) =>
        None
      case _ =>
        throw new IllegalArgumentException(
          "Inconsistent state: Only one of meta, payload returned for key[%s] version[%s]." format
            (key, version)
        )
    }
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
   * Extract bytes from the response.
   */
  private def asBytes(response: HttpResponse): Array[Byte] = {
    val byteBuffer = response.getContent.toByteBuffer
    val bytes = Array.ofDim[Byte](byteBuffer.remaining())
    byteBuffer.get(bytes)
    bytes
  }

  /**
   * Parse response content as JSON.
   */
  private def parseJson[A: ClassManifest](response: HttpResponse): A = {
    Jackson.parse[A](asBytes(response))
  }
}
