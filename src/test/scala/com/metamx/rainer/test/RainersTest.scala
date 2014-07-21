package com.metamx.rainer.test

import com.metamx.common.scala.Jackson
import com.metamx.rainer.test.helper.{RainerTests, TestPayload}
import com.metamx.rainer.{Commit, InMemoryCommitStorage, Rainers}
import com.simple.simplespec.Matchers
import org.joda.time.DateTime
import org.junit.Test
import com.metamx.common.scala.Predef._

class RainersTest extends Matchers with RainerTests
{
  def TP(s: String) = {
    Some(Jackson.bytes(TestPayload(s)))
  }

  @Test
  def testEmptiness() {
    withCluster {
      cluster =>
        withCurator(cluster) {
          curator =>
            Rainers.create[TestPayload](curator, "/hey", new InMemoryCommitStorage[TestPayload]).withFinally(_.stop()) {
              rainers =>
                val commit1 = Commit[TestPayload]("what", 1, TP("xxx"), "nobody", "nothing", new DateTime(1))
                rainers.start()
                rainers.storage.save(commit1)
                rainers.storage.get(commit1.key) must be(Some(commit1))
                rainers.keeper.get(commit1.key) must be(Some(commit1))
            }
        }
    }
  }
}
