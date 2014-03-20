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

import com.metamx.common.scala.Logging
import com.metamx.rainer.Commit
import java.util.concurrent.atomic.AtomicReference
import org.scalatra.{ActionResult, ScalatraServlet}

/**
 * Mixin trait to make [[com.metamx.rainer.http.RainerServlet]] lightnight fast
 *
 * ==Example==
 *
 * {{{
 *   val servlet = new RainerServlet[ValueType] with RainerMirrorServlet[ValueType] {
 *      override def commitStorage = myCommitStorage
 *      override def valueDeserialization = myValueDeserialization
 *      override def mirror = myMirror
 * }}}
 *
 */
trait RainerMirrorServlet[ValueType] extends ScalatraServlet with RainerServletUtils with Logging
{
  def mirror: AtomicReference[Map[String, Commit[ValueType]]]

  private def getMirror(key: String) = {
    mirror.get().get(key)
  }

  private def addHeader(res: ActionResult) = {
    res.copy(headers = res.headers + ("X-Rainer-Cached" -> "Yes"))
  }

  get("/:key") {
    addHeader(doGet(getMirror(params("key"))))
  }

  get("/:key/meta") {
    addHeader(doGetMeta(getMirror(params("key"))))
  }
}
