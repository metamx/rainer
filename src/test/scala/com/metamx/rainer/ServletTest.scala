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

import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.net.uri._
import com.metamx.common.scala.timekeeper.Timekeeper
import com.metamx.common.scala.untyped.Dict
import com.metamx.rainer.http.RainerServlet
import com.simple.simplespec.Spec
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.{InetResolver, Group}
import java.util.UUID
import org.joda.time.DateTime
import org.junit.Test
import org.scalatra.test.ScalatraTests

class ServletTest extends Spec
{

  class StandaloneTests
  {
    @Test
    def testEmptiness() {
      execute {
        tester =>
          tester.get("/things") {
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
            "X-Rainer-Author" -> "dude",
            "X-Rainer-Comment" -> "stuff",
            "X-Rainer-Empty" -> "Yes"
          )) {
            Jackson.parse[Dict](tester.body) must be(Map(
              "key" -> "what",
              "version" -> 2,
              "author" -> "dude",
              "comment" -> "stuff",
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
          tester.get("/things/what") {
            tester.body must be("""Key not found""")
            tester.status must be(404)
          }
          tester.get("/things/what/meta") {
            Jackson.parse[Dict](tester.body) must be(Map(
              "key" -> "what",
              "version" -> 2,
              "author" -> "dude",
              "comment" -> "stuff",
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
            tester.body must contain("Concurrent modification")
            tester.status must be(400)
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
  }

  class HttpCommitStorageTests extends CommitStorageTests {
    def makeClient[ValueType: KeyValueDeserialization](tester: ScalatraTests, suffix: String) = {
      val uri = new URI(tester.baseUrl) withPath (_.stripSuffix("/") + suffix)
      val client = ClientBuilder()
        .name(uri.toString)
        .codec(Http())
        .group(Group.fromVarAddr(InetResolver.bind(uri.authority)))
        .hostConnectionLimit(2)
        .daemon(true)
        .build()
      new HttpCommitStorage[ValueType](client, uri)
    }

    override def withStorage(f: (CommitStorage[TestPayload]) => Unit) {
      execute {
        tester =>
          val storage = makeClient[TestPayload](tester, "/things")
          f(storage)
      }
    }

    override def withPairedStorages(f: (CommitStorage[TestPayload], CommitStorage[TestPayloadStrict]) => Unit) {
      execute {
        tester =>
          val storage = makeClient[TestPayload](tester, "/things")
          val storageStrict = makeClient[TestPayloadStrict](tester, "/things")
          f(storage, storageStrict)
      }
    }
  }

  def execute(f: ScalatraTests => Unit) {
    val db = new DerbyMemoryDB(UUID.randomUUID().toString)
    db.start
    try {
      val storage = new DbCommitStorage[TestPayload](db, "rainer")
      storage.start()
      val servlet = new RainerServlet[TestPayload] {
        override def root = "/things"

        override def valueDeserialization = implicitly[KeyValueDeserialization[TestPayload]]

        override def commitStorage = storage

        override val timekeeper = new Timekeeper {
          override def now = new DateTime(2)
        }
      }
      new ScalatraTests {} withEffect {
        tests =>
          tests.addServlet(servlet, "/*")
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

}
