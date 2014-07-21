package com.metamx.rainer.test

import com.metamx.rainer.test.helper.{CannotPairStoragesException, CommitStorageTests, TestPayload, TestPayloadStrict}
import com.metamx.rainer.{CommitStorage, InMemoryCommitStorage}

class InMemoryCommitStorageTest extends CommitStorageTests
{
  override def withStorage(f: (CommitStorage[TestPayload]) => Unit) {
    f(new InMemoryCommitStorage)
  }

  override def withPairedStorages(f: (CommitStorage[TestPayload], CommitStorage[TestPayloadStrict]) => Unit) = {
    throw new CannotPairStoragesException
  }
}
