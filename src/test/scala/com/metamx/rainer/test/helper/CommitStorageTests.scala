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

import com.fasterxml.jackson.databind.JsonMappingException
import com.metamx.common.scala.Jackson
import com.metamx.rainer.{CommitOrderingException, Commit, CommitStorage}
import com.simple.simplespec.Matchers
import org.joda.time.DateTime
import org.junit.Test

trait CommitStorageTests extends Matchers
{
  def TP(s: String) = {
    Some(Jackson.bytes(TestPayload(s)))
  }

  def withStorage(f: CommitStorage[TestPayload] => Unit)

  def withPairedStorages(f: (CommitStorage[TestPayload], CommitStorage[TestPayloadStrict]) => Unit)

  @Test
  def testEmptiness()
  {
    withStorage {
      storage =>
        storage.heads must be(Map.empty[Commit.Key, Commit[TestPayload]])
        storage.headsNonEmpty must be(Map.empty[Commit.Key, Commit[TestPayload]])
        storage.get("hey") must be(None)
        storage.get("hey", 1) must be(None)
    }
  }

  @Test
  def testSave()
  {
    withStorage {
      storage =>
        val commit1 = Commit[TestPayload]("what", 1, TP("xxx"), "nobody", "nothing", new DateTime(1))
        val commit2 = Commit[TestPayload]("what", 2, TP("xxx"), "nobody", "nothing", new DateTime(1))
        storage.save(commit1)
        storage.save(commit2)
        storage.heads must be(Map("what" -> commit2))
        storage.headsNonEmpty must be(Map("what" -> commit2))
        storage.get("what") must be(Some(commit2))
        storage.get("what", 1) must be(Some(commit1))
        storage.get("what", 2) must be(Some(commit2))
        storage.get("what", 3) must be(None)
    }
  }

  @Test
  def testSaveInvalidSequence()
  {
    withStorage {
      storage =>
        val commit1 = Commit[TestPayload]("what", 1, TP("xxx"), "nobody", "nothing", new DateTime(1))
        val commit3 = Commit[TestPayload]("what", 3, TP("xxx"), "nobody", "nothing", new DateTime(1))
        evaluating {
          storage.save(commit3)
        } must throwA[CommitOrderingException](""".*Concurrent modification: what: .*""".r)
        storage.save(commit1)
        evaluating {
          storage.save(commit1)
        } must throwA[CommitOrderingException](""".*Concurrent modification: what: .*""".r)
        evaluating {
          storage.save(commit3)
        } must throwA[CommitOrderingException](""".*Concurrent modification: what: .*""".r)
        storage.heads must be(Map("what" -> commit1))
        storage.headsNonEmpty must be(Map("what" -> commit1))
        storage.get("what") must be(Some(commit1))
        storage.get("what", 1) must be(Some(commit1))
        storage.get("what", 2) must be(None)
        storage.get("what", 3) must be(None)
    }
  }

  @Test
  def testSaveEmpty()
  {
    withStorage {
      storage =>
        val commit1 = Commit[TestPayload]("what", 1, TP("xxx"), "nobody", "nothing", new DateTime(1))
        val commit2 = Commit[TestPayload]("what", 2, None, "nobody", "nothing", new DateTime(1))
        storage.save(commit1)
        storage.save(commit2)
        storage.heads must be(Map("what" -> commit2))
        storage.headsNonEmpty must be(Map.empty[Commit.Key, Commit[TestPayload]])
        storage.get("what") must be(Some(commit2))
        storage.get("what", 1) must be(Some(commit1))
        storage.get("what", 2) must be(Some(commit2))
        storage.get("what", 3) must be(None)
    }
  }

  @Test
  def testRestoreCorrupt()
  {
    try {
      withPairedStorages {
        (storage, storageStrict) =>
          val theCommit = Commit[TestPayload]("what", 1, TP("xxx"), "nobody", "nothing", new DateTime(1))
          storage.save(theCommit)
          storage.get("what") must be(Some(theCommit))
          val strictCommit = storageStrict.get("what").get
          strictCommit.key must be("what")
          strictCommit.version must be(1)
          strictCommit.author must be("nobody")
          strictCommit.comment must be("nothing")
          strictCommit.mtime must be(new DateTime(1))
          val eOption = strictCommit.value.get.left.toOption
          eOption.isDefined must be(true)
          eOption.get must beA[JsonMappingException]
          eOption.get.getMessage must contain("string length must be even")
      }
    } catch {
      case e: CannotPairStoragesException =>
        // Suppress.
    }
  }

  @Test
  def testPostSaveHook()
  {
    withStorage {
      storage =>
        var commits = 0
        val hookedStorage = CommitStorage.withPostSaveHook(storage) {
          commit =>
            commits += 1
        }
        val theCommit = Commit[TestPayload]("what", 1, TP("xxx"), "nobody", "nothing", new DateTime(1))
        hookedStorage.save(theCommit)
        evaluating {
          hookedStorage.save(theCommit)
        } must throwA[CommitOrderingException](""".*Concurrent modification: what: .*""".r)
        commits must be(1)
    }
  }
}

class CannotPairStoragesException extends Exception
