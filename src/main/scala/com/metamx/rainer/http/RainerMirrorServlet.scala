package com.metamx.rainer.http

import java.util.concurrent.atomic.AtomicReference
import com.metamx.rainer.Commit
import org.scalatra.{ActionResult, ScalatraServlet}
import com.metamx.common.scala.Logging

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

  def getMirror(key: String) = {
    mirror.get().get(key)
  }

  def addHeader(res: ActionResult) = {
    res.copy(headers = res.headers + ("X-Rainer-Cached" -> "Yes"))
  }

  get("/:key") {
    addHeader(doGet(getMirror(params("key"))))
  }

  get("/:key/meta") {
    addHeader(doGetMeta(getMirror(params("key"))))
  }
}
