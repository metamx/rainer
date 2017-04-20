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

package com.metamx.rainer.test

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped.Dict
import com.metamx.rainer._
import com.metamx.rainer.http.{RainerMirrorServlet, RainerServlet}
import com.metamx.rainer.test.helper.{DerbyCommitTable, DerbyMemoryDB, RainerTests, TestPayload}
import com.simple.simplespec.Matchers
import com.twitter.util.{Await, Var, Witness}
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.Test
import org.scalatra.test.ScalatraTests

object ServletTest extends RainerTests
{
  def execute(f: ScalatraTests => Unit) {
    val db = new DerbyMemoryDB(UUID.randomUUID().toString) with DerbyCommitTable
    db.start
    try {
      val storage = new DbCommitStorage[TestPayload](db, "rainer")
      storage.start()

      val servlet = new RainerServlet[TestPayload] {
        override def valueDeserialization = implicitly[KeyValueDeserialization[TestPayload]]
        override def commitStorage = storage

        override val timekeeper = new Timekeeper {
          override def now = new DateTime(2)
        }
      }
      new ScalatraTests {} withEffect {
        tests =>
          tests.addServlet(servlet, "/things/*")
          tests.start()
          try f(tests)
          finally {
            tests.stop()
          }
      }
    }
    finally {
      db.stop
    }
  }

  def executeMirror(f: ScalatraTests => Unit) {
    withCluster {
      cluster =>
        withCurator(cluster) {
          curator =>

            val db = new DerbyMemoryDB(UUID.randomUUID().toString) with DerbyCommitTable
            db.start
            try {
              val keeper = new CommitKeeper[TestPayload](curator, "/zk/path")
              val storage = CommitStorage.keeperPublishing(new DbCommitStorage[TestPayload](db, "rainer"), keeper)
              storage.start()

              val keeperMirror: Var[Map[String, Commit[TestPayload]]] = keeper.mirror()
              val mirrorRef = new AtomicReference[Map[String, Commit[TestPayload]]]
              val c = keeperMirror.changes.register(Witness(mirrorRef))

              val servlet = new RainerServlet[TestPayload] with RainerMirrorServlet[TestPayload] {
                override def valueDeserialization = implicitly[KeyValueDeserialization[TestPayload]]
                override def commitStorage = storage
                override def mirror = mirrorRef

                override val timekeeper = new Timekeeper {
                  override def now = new DateTime(2)
                }
              }
              new ScalatraTests {} withEffect {
                tests =>
                  tests.addServlet(servlet, "/things/*")
                  tests.start()
                  try f(tests)
                  finally {
                    tests.stop()
                  }
              }
              Await.result(c.close())
            }
            finally {
              db.stop
            }
        }
    }
  }

}

class ServletTest extends Matchers with RainerTests
{

  import ServletTest.{execute, executeMirror}

  @Test
  def testEmptiness() {
    execute {
      tester =>
        tester.get("/things") {
          tester.body must be("{}\r\n")
          tester.status must be(200)
        }
        tester.get("/things?all=yes") {
          tester.body must be("{}\r\n")
          tester.status must be(200)
        }
        tester.get("/things/what") {
          tester.body must be("Key not found")
          tester.status must be(404)
        }
        tester.get("/things/what/1") {
          tester.body must be("Key not found")
          tester.status must be(404)
        }
        tester.get("/things/what/meta") {
          tester.body must be("Key not found")
          tester.status must be(404)
        }
        tester.get("/things/what/1/meta") {
          tester.body must be("Key not found")
          tester.status must be(404)
        }
    }
  }

  @Test
  def testPostThenGet() {
    execute {
      tester =>
        tester.post("/things/what/1", """{"s": "hey"}""".getBytes, Map(
          "X-Rainer-Author" -> "dude",
          "X-Rainer-Comment" -> "stuff"
        )) {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 1,
            "author" -> "dude",
            "comment" -> "stuff",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        // GETs
        tester.get("/things") {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "what" -> Map(
                "key" -> "what",
                "version" -> 1,
                "author" -> "dude",
                "comment" -> "stuff",
                "mtime" -> "1970-01-01T00:00:00.002Z",
                "empty" -> false
              )
            )
          )
          tester.status must be(200)
        }
        tester.get("/things?payload_utf8=yes") {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "what" -> Map(
                "key" -> "what",
                "version" -> 1,
                "author" -> "dude",
                "comment" -> "stuff",
                "mtime" -> "1970-01-01T00:00:00.002Z",
                "empty" -> false,
                "payload_utf8" -> """{"s": "hey"}"""
              )
            )
          )
          tester.status must be(200)
        }
        tester.get("/things?payload_base64=yes") {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "what" -> Map(
                "key" -> "what",
                "version" -> 1,
                "author" -> "dude",
                "comment" -> "stuff",
                "mtime" -> "1970-01-01T00:00:00.002Z",
                "empty" -> false,
                "payload_base64" -> "eyJzIjogImhleSJ9"
              )
            )
          )
          tester.status must be(200)
        }
        tester.get("/things/what") {
          tester.body must be("""{"s": "hey"}""")
          tester.status must be(200)
        }
        tester.get("/things/what/1") {
          tester.body must be("""{"s": "hey"}""")
          tester.status must be(200)
          tester.header("X-Rainer-Key") must be ("what")
          tester.header("X-Rainer-Version") must be ("1")
          tester.header("X-Rainer-Author") must be ("dude")
          tester.header("X-Rainer-Comment") must be ("stuff")
          tester.header("X-Rainer-Modified-Time") must be ("1970-01-01T00:00:00.002Z")
          tester.header("X-Rainer-Empty") must be ("No")
        }
        tester.get("/things/what/2") {
          tester.body must be("""Key not found""")
          tester.status must be(404)
        }
        tester.get("/things/what/meta") {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 1,
            "author" -> "dude",
            "comment" -> "stuff",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        tester.get("/things/what/1/meta") {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 1,
            "author" -> "dude",
            "comment" -> "stuff",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        tester.get("/things/what/2/meta") {
          tester.body must be("Key not found")
          tester.status must be(404)
        }
    }
  }

  @Test
  def testDoublePostThenGet() {
    execute {
      tester =>
        tester.post("/things/what/1", """{"s": "hey"}""".getBytes, Map(
          "X-Rainer-Author" -> "dude",
          "X-Rainer-Comment" -> "stuff"
        )) {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 1,
            "author" -> "dude",
            "comment" -> "stuff",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        tester.post("/things/what/2", """{"s": "rofl"}""".getBytes, Map(
          "X-Rainer-Author" -> "xxx",
          "X-Rainer-Comment" -> "yyy"
        )) {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 2,
            "author" -> "xxx",
            "comment" -> "yyy",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        // GETs
        tester.get("/things/what") {
          tester.body must be("""{"s": "rofl"}""")
          tester.status must be(200)
        }
        tester.get("/things/what/1") {
          tester.body must be("""{"s": "hey"}""")
          tester.status must be(200)
        }
        tester.get("/things/what/2") {
          tester.body must be("""{"s": "rofl"}""")
          tester.status must be(200)
        }
        tester.get("/things/what/meta") {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 2,
            "author" -> "xxx",
            "comment" -> "yyy",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        tester.get("/things/what/1/meta") {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 1,
            "author" -> "dude",
            "comment" -> "stuff",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        tester.get("/things/what/2/meta") {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 2,
            "author" -> "xxx",
            "comment" -> "yyy",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
    }
  }

  @Test
  def testPostEmpty() {
    execute {
      tester =>
        tester.post("/things/what/1", """{"s": "hey"}""".getBytes, Map(
          "X-Rainer-Author" -> "dude",
          "X-Rainer-Comment" -> "stuff"
        )) {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 1,
            "author" -> "dude",
            "comment" -> "stuff",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> false
          ))
          tester.status must be(200)
        }
        tester.post("/things/what/2", "".getBytes, Map(
          "X-Rainer-Author" -> "duder",
          "X-Rainer-Comment" -> "hey",
          "X-Rainer-Empty" -> "Yes"
        )) {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 2,
            "author" -> "duder",
            "comment" -> "hey",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> true
          ))
          tester.status must be(200)
        }
        // GETs
        tester.get("/things") {
          tester.body must be("{}\r\n")
          tester.status must be(200)
        }
        tester.get("/things?all=yes") {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "what" -> Map(
                "key" -> "what",
                "version" -> 2,
                "author" -> "duder",
                "comment" -> "hey",
                "mtime" -> "1970-01-01T00:00:00.002Z",
                "empty" -> true
              )
            )
          )
          tester.status must be(200)
        }
        tester.get("/things/what") {
          tester.body must be("")
          tester.header("X-Rainer-Empty") must be("Yes")
          tester.status must be(200)
        }
        tester.get("/things/what/meta") {
          Jackson.parse[Dict](tester.body) must be(Map(
            "key" -> "what",
            "version" -> 2,
            "author" -> "duder",
            "comment" -> "hey",
            "mtime" -> "1970-01-01T00:00:00.002Z",
            "empty" -> true
          ))
          tester.status must be(200)
        }
    }
  }

  @Test
  def testPostBadVersion() {
    execute {
      tester =>
        tester.post("/things/what/2", """{"s":"hey"}""".getBytes, Map(
          "X-Rainer-Author" -> "dude",
          "X-Rainer-Comment" -> "stuff"
        )) {
          Jackson.parse[Dict](tester.body) must
            be(Map("conflict" -> Map("key" -> "what", "providedVersion" -> 2, "expectedVersion" -> 1)))
          tester.status must be(409)
        }
    }
  }

  @Test
  def testPostBadPayload() {
    execute {
      tester =>
        tester.post("/things/what/1", """lol""".getBytes, Map(
          "X-Rainer-Author" -> "dude",
          "X-Rainer-Comment" -> "stuff"
        )) {
          tester.body must contain("Malformed input: com.fasterxml.jackson.core.JsonParseException")
          tester.status must be(400)
        }
    }
  }

  @Test
  def testPostMissingAuthor() {
    execute {
      tester =>
        tester.post("/things/what/1", """{"s":"hey"}""".getBytes, Map(
          "X-Rainer-Comment" -> "stuff"
        )) {
          tester.body must contain("Missing header: X-Rainer-Author")
          tester.status must be(400)
        }
    }
  }

  @Test
  def testPostMissingComment() {
    execute {
      tester =>
        tester.post("/things/what/1", """{"s":"hey"}""".getBytes, Map(
          "X-Rainer-Author" -> "dude"
        )) {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "key" -> "what",
              "version" -> 1,
              "author" -> "dude",
              "comment" -> "",
              "mtime" -> "1970-01-01T00:00:00.002Z",
              "empty" -> false
            )
          )
          tester.status must be(200)
        }
    }

    execute {
      tester =>
        tester.post("/things/what/1", """{"s":"hey"}""".getBytes, Map(
          "X-Rainer-Author" -> "dude",
          "X-Rainer-Comment" -> ""
        )) {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "key" -> "what",
              "version" -> 1,
              "author" -> "dude",
              "comment" -> "",
              "mtime" -> "1970-01-01T00:00:00.002Z",
              "empty" -> false
            )
          )
          tester.status must be(200)
        }
    }
  }

  @Test
  def testGetMirror() {
    executeMirror {
      tester =>
        tester.post(
          "/things/what/1", """{"s": "hey"}""".getBytes, Map(
            "X-Rainer-Author" -> "dude",
            "X-Rainer-Comment" -> "stuff"
          )
        ) {
          Jackson.parse[Dict](tester.body) must be(
            Map(
              "key" -> "what",
              "version" -> 1,
              "author" -> "dude",
              "comment" -> "stuff",
              "mtime" -> "1970-01-01T00:00:00.002Z",
              "empty" -> false
            )
          )
          tester.status must be(200)
        }

        within(2.seconds) {
          tester.get("/things") {
            tester.status must be(200)
            tester.header.get("X-Rainer-Cached") must be(Some("Yes"))
            Jackson.parse[Dict](tester.body) must be(
              Map(
                "what" -> Map(
                  "key" -> "what",
                  "version" -> 1,
                  "author" -> "dude",
                  "comment" -> "stuff",
                  "mtime" -> "1970-01-01T00:00:00.002Z",
                  "empty" -> false
                )
              )
            )
          }
          tester.get("/things/what") {
            tester.body must be( """{"s": "hey"}""")
            tester.status must be(200)
            tester.header.get("X-Rainer-Cached") must be(Some("Yes"))
            tester.header("X-Rainer-Version") must be("1")
          }
          tester.get("/things/what/1") {
            tester.body must be( """{"s": "hey"}""")
            tester.status must be(200)
            tester.header.get("X-Rainer-Cached") must be(None)
            tester.header("X-Rainer-Version") must be("1")
          }
          tester.get("/things/what/meta") {
            tester.status must be(200)
            tester.header.get("X-Rainer-Cached") must be(Some("Yes"))
            Jackson.parse[Dict](tester.body) must be(
              Map(
                "key" -> "what",
                "version" -> 1,
                "author" -> "dude",
                "comment" -> "stuff",
                "mtime" -> "1970-01-01T00:00:00.002Z",
                "empty" -> false
              )
            )
          }
          tester.get("/things/what/1/meta") {
            tester.status must be(200)
            tester.header.get("X-Rainer-Cached") must be(None)
            Jackson.parse[Dict](tester.body) must be(
              Map(
                "key" -> "what",
                "version" -> 1,
                "author" -> "dude",
                "comment" -> "stuff",
                "mtime" -> "1970-01-01T00:00:00.002Z",
                "empty" -> false
              )
            )
          }
        }
    }
  }

}
