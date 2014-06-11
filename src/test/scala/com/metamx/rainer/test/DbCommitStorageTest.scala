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

import com.metamx.common.scala.db.DB
import com.metamx.rainer.test.helper.{CommitStorageTests, TestPayloadStrict, TestPayload, DerbyCommitTableWithoutIsEmpty, DerbyCommitTable, DerbyMemoryDB}
import com.metamx.rainer.{DbCommitStorageMixin, CommitStorage, DbCommitStorage}
import java.util.UUID

class DerbyTest extends DerbyTests {
  override def createDb() = new DerbyMemoryDB(UUID.randomUUID().toString) with DerbyCommitTable
}

class DerbyWithoutIsEmptyTest extends DerbyTests {
  override def createDb() = new DerbyMemoryDB(UUID.randomUUID().toString) with DerbyCommitTableWithoutIsEmpty
}

trait DerbyTests extends CommitStorageTests
{
  def createDb(): DB with DbCommitStorageMixin

  def withDb[A](f: DB with DbCommitStorageMixin => A): A = {
    val db = createDb()
    db.start
    try f(db) finally {
      db.stop
    }
  }

  override def withPairedStorages(f: (CommitStorage[TestPayload], CommitStorage[TestPayloadStrict]) => Unit) {
    withDb {
      db =>
        val storage = new DbCommitStorage[TestPayload](db, "rainer")
        val storageStrict = new DbCommitStorage[TestPayloadStrict](db, "rainer")
        storage.start() // Only start one, starting both throws exceptions with Derby
        try {
          f(storage, storageStrict)
        } finally {
          storageStrict.stop()
        }
    }
  }

  override def withStorage(f: (CommitStorage[TestPayload]) => Unit) {
    withDb {
      db =>
        val storage = new DbCommitStorage[TestPayload](db, "rainer")
        storage.start()
        try {
          f(storage)
        } finally {
          storage.stop()
        }
    }
  }
}
