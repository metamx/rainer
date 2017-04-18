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

package com.metamx.rainer.test.helper

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.rainer.{Commit, CommitKeeper}
import com.twitter.util.{Closable, Witness}
import java.util.concurrent.atomic.AtomicReference
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingCluster
import org.joda.time.DateTime

trait RainerTests extends Logging
{

  def newCurator(zkConnect: String) = {
    CuratorFrameworkFactory
      .builder()
      .connectString(zkConnect)
      .sessionTimeoutMs(new Duration("PT10S").getMillis.toInt)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3, 2000))
      .build()
  }

  def withCluster[A](f: TestingCluster => A): A = {
    val cluster = new TestingCluster(1)
    cluster.start()
    try f(cluster) finally {
      cluster.stop()
    }
  }

  def withCurator[A](cluster: TestingCluster)(f: CuratorFramework => A): A = {
    val curator = newCurator(cluster.getConnectString)
    curator.start()
    try f(curator) finally {
      curator.close()
    }
  }

  def asMap[A](ck: CommitKeeper[A]): (Closable, AtomicReference[Map[Commit.Key, Commit[A]]]) = {
    val ref = new AtomicReference[Map[Commit.Key, Commit[A]]]()
    val c = ck.mirror().changes.register(Witness(ref))
    (c, ref)
  }

  def within(t: Period)(f: => Unit) {
    val end = DateTime.now + t
    var looping = true
    while (looping) {
      try {
        f
        looping = false
      }
      catch {
        case e: AssertionError if DateTime.now < end =>
          log.warn("Assertion failed, will try again soon.")
          Thread.sleep(500)
      }
    }
  }
}
