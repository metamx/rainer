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
 * Uses [[com.metamx.rainer.CommitKeeper]] for read requests and
 * [[com.metamx.rainer.CommitStorage]] for write requests
 *
 * ==Example==
 *
 * {{{
 * implicit val deserialization = KeyValueDeserialization.usingJackson[ValueType](objectMapper)
 *
 * val keeper = new CommitKeeper[ValueType](zkClient, "/path/in/zk")
 * val unerlyingStorage = new DbCommitStorage[ValueType](db, "table_name")
 * val wrappedStorage = CommitStorage.keeperPublishing(unerlyingStorage, keeper)
 *
 * val keeperMirror: Var[Map[String, Commit[ValueType]]] = keeper.mirror()
 * val mirrorRef = new AtomicReference[Map[Pipeline.Name, Commit[Pipeline]]]
 * keeperMirror.changes.register(Witness(mirrorRef))
 *
 * val servlet = new RainerServlet[ValueType] with RainerMirrorServlet[ValueType] {
 *    override def commitStorage = wrappedStorage
 *    override def valueDeserialization = implicitly[KeyValueDeserialization[DictValue]]
 *    override def mirror = mirrorRef
 * }}}
 *
 */
trait RainerMirrorServlet[ValueType] extends ScalatraServlet with RainerServletBase with Logging
{
  def mirror: AtomicReference[Map[String, Commit[ValueType]]]

  private def addHeader(res: ActionResult) = {
    res.copy(headers = res.headers + ("X-Rainer-Cached" -> "Yes"))
  }

  get("/") {
    addHeader(doList(mirror.get()))
  }

  get("/:key") {
    addHeader(doGet(mirror.get().get(params("key"))))
  }

  get("/:key/meta") {
    addHeader(doGetMeta(mirror.get().get(params("key"))))
  }
}
