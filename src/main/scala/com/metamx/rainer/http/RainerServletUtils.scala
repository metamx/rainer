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

import org.scalatra.{NotFound, Ok, ScalatraServlet}
import com.metamx.rainer.Commit
import com.metamx.common.scala.Jackson
import com.google.common.base.Charsets
import java.nio.charset.{CharacterCodingException, CodingErrorAction}
import java.nio.ByteBuffer

trait RainerServletUtils {
  self: ScalatraServlet =>

  def doGet[ValueType](commitOption: Option[Commit[ValueType]]) = {
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

  def doGetMeta[ValueType](commitOption: Option[Commit[ValueType]]) = {
    commitOption match {
      case Some(commit) =>
        Ok(json(commit.metadata))
      case None =>
        NotFound("Key not found")
    }
  }

  def json[A](x: A) = {
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
