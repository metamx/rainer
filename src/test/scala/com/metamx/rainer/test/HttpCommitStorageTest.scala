package com.metamx.rainer.test

import com.metamx.common.scala.net.uri._
import com.metamx.rainer.test.helper.{TestPayloadStrict, CommitStorageTests, TestPayload}
import com.metamx.rainer.{CommitStorage, HttpCommitStorage, KeyValueDeserialization}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Http
import com.twitter.finagle.{InetResolver, Group}
import org.scalatra.test.ScalatraTests

class HttpCommitStorageTest extends CommitStorageTests {
  def makeClient[ValueType: KeyValueDeserialization](tester: ScalatraTests, suffix: String) = {
    val uri = new URI(tester.baseUrl) withPath (_.stripSuffix("/") + suffix)
    val client = ClientBuilder()
      .name(uri.toString)
      .codec(Http())
      .group(Group.fromVarAddr(InetResolver().bind(uri.authority)))
      .hostConnectionLimit(2)
      .daemon(true)
      .build()
    new HttpCommitStorage[ValueType](client, uri)
  }

  override def withStorage(f: (CommitStorage[TestPayload]) => Unit) {
    ServletTest.execute {
      tester =>
        val storage = makeClient[TestPayload](tester, "/things")
        f(storage)
    }
  }

  override def withPairedStorages(f: (CommitStorage[TestPayload], CommitStorage[TestPayloadStrict]) => Unit) {
    ServletTest.execute {
      tester =>
        val storage = makeClient[TestPayload](tester, "/things")
        val storageStrict = makeClient[TestPayloadStrict](tester, "/things")
        f(storage, storageStrict)
    }
  }
}
