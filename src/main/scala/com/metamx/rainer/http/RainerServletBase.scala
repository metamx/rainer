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
import com.google.common.io.BaseEncoding
import com.metamx.common.scala.Jackson
import com.metamx.rainer.Commit
import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import org.scalatra.{BadRequest, NotFound, Ok, ScalatraServlet}

trait RainerServletBase {
  self: ScalatraServlet =>

  protected def shouldListAll = RainerServlet.yesNo(params.getOrElse("all", "false"))

  protected def doList[ValueType](heads: Map[Commit.Key, Commit[ValueType]]) = {
    val payloadUtf8Option = RainerServlet.yesNo(params.getOrElse("payload_utf8", "false"))
    val payloadBase64Option = RainerServlet.yesNo(params.getOrElse("payload_base64", "false"))
    val allOption = shouldListAll
    (allOption, payloadUtf8Option, payloadBase64Option) match {
      case (None, _, _) => BadRequest("Invalid 'all' parameter (should be boolean)")
      case (_, None, _) => BadRequest("Invalid 'payload_utf8' parameter (should be boolean)")
      case (_, _, None) => BadRequest("Invalid 'payload_base64' parameter (should be boolean)")
      case (Some(all), Some(payloadUtf8), Some(payloadBase64)) =>
        Ok(json {
          // Still need to filter empty commits when !all, because our caller might not have done it.
          for ((k, commit) <- heads if all || !commit.isEmpty) yield {
            (k, commit.meta.asMap ++ (if (commit.payload.isDefined && payloadUtf8) {
              Some("payload_utf8" -> new String(commit.payload.get, Charsets.UTF_8))
            } else {
              None
            }) ++ (if (commit.payload.isDefined && payloadBase64) {
              Some("payload_base64" -> BaseEncoding.base64().encode(commit.payload.get))
            } else {
              None
            }))
          }
        })
    }
  }

  protected def doGet[ValueType](commitOption: Option[Commit[ValueType]]) = {
    commitOption match {
      case Some(commit) =>
        val payload = commit.payload.getOrElse(Array.empty[Byte])
        Ok(textIfPossible(payload), RainerServlet.headersForCommitMetadata(commit.meta))
      case _ =>
        NotFound("Key not found")
    }
  }

  protected def doGetMeta[ValueType](commitOption: Option[Commit[ValueType]]) = {
    commitOption match {
      case Some(commit) =>
        Ok(json(commit.meta.asMap))
      case None =>
        NotFound("Key not found")
    }
  }

  protected def json[A](x: A) = {
    contentType = "application/json"
    Jackson.generate(x) + "\r\n"
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
}
