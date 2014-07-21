package com.metamx.rainer

import com.metamx.common.scala.collection.implicits._
import com.metamx.rainer.Commit._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A commit storage implementation that stores everything in memory. Nothing is persisted anywhere, and restarts
 * destroy all data.
 */
class InMemoryCommitStorage[ValueType] extends CommitStorage[ValueType]
{
  private val lock = new AnyRef

  private val commits = mutable.HashMap[Commit.Key, ArrayBuffer[Commit[ValueType]]]()

  override def start() {}

  override def stop() {
    lock.synchronized {
      commits.clear()
    }
  }

  override def heads = lock.synchronized {
    commits.toMap.strictMapValues(_.last)
  }

  override def get(key: Key) = lock.synchronized {
    commits.get(key).map(_.last)
  }

  override def get(key: Key, version: Int) = lock.synchronized {
    commits.get(key).flatMap(_ lift (version - 1))
  }

  override def headsNonEmpty = lock.synchronized {
    heads filter {
      case (k, v) =>
        v.payload.isDefined
    }
  }

  override def save(commit: Commit[ValueType]) = lock.synchronized {
    val nextVersion = commits.get(commit.key).map(_.size + 1).getOrElse(1)
    if (commits.get(commit.key).size != commit.version - 1) {
      throw new IllegalArgumentException(
        "Concurrent modification: %s: requested version (%s) was not next available version (%s)" format
          (commit.key, commit.version, nextVersion)
      )
    }
    commits.getOrElseUpdate(commit.key, ArrayBuffer[Commit[ValueType]]()) += commit
  }
}
