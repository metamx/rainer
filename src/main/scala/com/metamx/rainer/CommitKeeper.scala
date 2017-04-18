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

import com.github.nscala_time.time.Imports._
import com.metamx.common.scala.Logging
import com.metamx.common.scala.concurrent.everyFuzzy
import com.metamx.common.scala.exception._
import com.twitter.util.{Await, Closable, Future, Time, Var}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.recipes.leader.{LeaderSelector, LeaderSelectorListenerAdapter}
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.data.Stat
import scala.collection.JavaConverters._

/**
 * ZooKeeper-based storage and notification for each key's most current commit. This class does not implement
 * CommitStorage because even though many method signatures are similar, it does not provide fully journaled storage.
 * In particular, no history is kept, and commits with empty or unparseable values are not stored.
 */
class CommitKeeper[ValueType : KeyValueDeserialization](
  curator: CuratorFramework,
  zkPath: String
) extends Logging
{
  private def dataPath = ZKPaths.makePath(zkPath, "data")

  private def electionPath = ZKPaths.makePath(zkPath, "election")

  /**
   * Immutable, point-in-time view of all nonempty commits.
   */
  def heads: Map[Commit.Key, Commit[ValueType]] = {
    val children = curator.getChildren.forPath(dataPath).asScala swallow {
      case e: NoNodeException =>
    } getOrElse Nil
    (for {
      key <- children
      commit <- get(key)
    } yield {
      (key, commit)
    }).toMap
  }

  /**
   * Get the current version of some commit key. Returns None if the znode does not exist, but throws an exception
   * if the znode is present and unparseable. If the znode is parseable but the payload is not, returns a normal
   * Commit where value.isLeft.
   */
  def get(key: Commit.Key): Option[Commit[ValueType]] = {
    for {
      data <- curator.getData.forPath(znode(key)) swallow {
        case e: KeeperException.NoNodeException => // Suppress
      }
    } yield {
      fromKeyAndBytes(key, data)
    }
  }

  /**
   * Live-updating view of all present commits. The first value seen by the returned Var will be a fully-initialized
   * cache. Future values will reflect each update in the order they are seen. This behavior is backed by a Curator
   * PathChildrenCache.
   *
   * Note that absence of a commit in the map is not a guarantee of true absence of the commit. Commits can be
   * spuriously missing due to parsing errors.
   */
  def mirror(): Var[Map[Commit.Key, Commit[ValueType]]] = Var.async[Map[Commit.Key, Commit[ValueType]]](Map.empty) {
    updater =>
      // Synchronizes access to cacheReady and theMap; also allows notifications when the cache is ready.
      val cacheLock = new AnyRef
      var cacheReady = false
      var theMap = Map.empty[Commit.Key, Commit[ValueType]]

      val cache = new PathChildrenCache(curator, dataPath, true)
      cache.getListenable.addListener(
        new PathChildrenCacheListener
        {
          override def childEvent(_curator: CuratorFramework, event: PathChildrenCacheEvent) {
            def commitKey = ZKPaths.getNodeFromPath(event.getData.getPath)
            def commitPayload = event.getData.getData
            event.getType match {
              case INITIALIZED =>
                cacheLock synchronized {
                  cacheReady = true
                  cacheLock.notifyAll()
                  updater.update(theMap)
                }

              case CHILD_ADDED | CHILD_UPDATED =>
                val what = if (event.getType == CHILD_ADDED) "added" else "updated"
                val commitIfDeserializable = fromKeyAndBytes(commitKey, commitPayload).catchEither[Exception]
                commitIfDeserializable match {
                  case Right(value) =>
                    log.info("Commit %s: %s: %s", what, commitKey, value)
                    cacheLock synchronized {
                      theMap = theMap + (commitKey -> value)
                      if (cacheReady) {
                        updater.update(theMap)
                      }
                    }

                  case Left(e) =>
                    val trimThreshold = 1024
                    val trimmed = commitPayload match {
                      case bytes if bytes.length < trimThreshold => new String(bytes)
                      case bytes => new String(bytes.take(trimThreshold)) + " ..."
                    }
                    log.error(e, "Commit %s with bad serialization: %s, data = %s", what, commitKey, trimmed)
                    cacheLock synchronized {
                      theMap = theMap - commitKey
                      if (cacheReady) {
                        updater.update(theMap)
                      }
                    }
                }

              case CHILD_REMOVED =>
                log.info("Commit removed: %s", commitKey)
                cacheLock synchronized {
                  theMap = theMap - commitKey
                  if (cacheReady) {
                    updater.update(theMap)
                  }
                }

              case x =>
                log.debug("No action needed for PathChildrenCache event type: %s", x)
            }
          }
        }
      )
      cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)
      cacheLock synchronized {
        while (!cacheReady) {
          cacheLock.wait()
        }
      }
      new Closable {
        override def close(deadline: Time) = Future(cache.close())
      }
  }

  private def dataWithStat(path: String): Option[(Array[Byte], Stat)] = {
    val stat = new Stat
    curator.getData.storingStatIn(stat).forPath(path) swallow {
      case e: KeeperException.NoNodeException =>
    } map {
      data => (data, stat)
    }
  }

  /**
   * Publish a new commit to ZooKeeper. Prevents rollbacks, when possible, using the commit's version number.
   *
   * @note It's much more possible to prevent rollbacks when commit payloads are always nonempty. This is because
   *       empty commits are removed from ZooKeeper, which means we cannot distinguish them from commits that have
   *       simply never been published at all.
   */
  def save(commit: Commit[ValueType]) {
    val commitStatOption: Option[(Option[Commit[ValueType]], Stat)] = {
      dataWithStat(znode(commit.key)) map {
        ds =>
          val data = fromKeyAndBytes(commit.key, ds._1) swallow {
            case e: Exception =>
          }
          (data, ds._2)
      }
    }
    try {
      commitStatOption match {

        case Some((Some(zkCommit), zkStat)) => // Possibly update existing znode

          // fromKeyAndBytes should have suppressed bad values, so this should never throw.
          if (commit.isEmpty && commit.version > zkCommit.version) {
            log.info("Removing commit[%s] (znode[%s])", commit.key, znode(commit.key))
            curator.delete().withVersion(zkStat.getVersion).forPath(znode(commit.key))
          } else if (commit == zkCommit) {
            log.debug("No change to commit[%s] (version[%s] znode[%s])",
              commit.key, commit.version, znode(commit.key)
            )
          } else if (commit.version > zkCommit.version) {
            // This will refuse to update a commit if contents are different but versions are the same.
            log.info("Updating commit %s (from version[%s] to version[%s] znode[%s])",
              commit.key, zkCommit.version, commit.version, znode(commit.key)
            )
            curator
              .setData()
              .withVersion(zkStat.getVersion)
              .forPath(znode(commit.key), Commit.serialize(commit))
          } else {
            assert(commit.version <= zkCommit.version)
            throw new ConcurrentCommitException(
              "Refusing to update commit %s (from version[%s] to version[%s] znode[%s])" format (
                commit.key, zkCommit.version, commit.version, znode(commit.key)
              )
            )
          }

        case Some((_, zkStat)) => // Corrupt commit in current znode, force an update

          log.warn("Replacing corrupt commit %s (from version[unknown] to version[%s] znode[%s])",
            commit.key, commit.version, znode(commit.key)
          )

          if (commit.isEmpty) {
            curator.delete().withVersion(zkStat.getVersion).forPath(znode(commit.key))
          } else {
            curator
              .setData()
              .withVersion(zkStat.getVersion)
              .forPath(znode(commit.key), Commit.serialize(commit))
          }

        case None => // No existing znode for this commit

          if (commit.isEmpty) {
            log.debug("No need to create empty commit %s (version[%s] znode[%s])",
              commit.key, commit.version, znode(commit.key)
            )
          } else {
            log.info("Creating commit %s (version[%s] znode[%s])", commit.key, commit.version, znode(commit.key))
            curator.create().creatingParentsIfNeeded().forPath(znode(commit.key), Commit.serialize(commit))
          }

      }
    } catch {

      case e @ (_: KeeperException.BadVersionException | _: KeeperException.NodeExistsException) =>
        val zkCommit: Option[Commit[ValueType]] = commitStatOption.flatMap(_._1)
        throw new ConcurrentCommitException(
          "Concurrent modification when trying to update commit: %s "
            + "(from version[%s] to version[%s] znode[%s]): %s" format (
            commit.key, zkCommit.map(_.version).getOrElse("unknown"), commit.version, znode(commit.key), e
          ))

    }
  }

  /**
   * Runnable that automatically publishes commits from storage to ZK.
   */
  def autoPublisher(
    storage: CommitStorage[ValueType],
    period: Duration,
    periodFuzz: Double,
    delay: Boolean = true,
    errorHandler: (Throwable, Commit[ValueType]) => Unit = (e: Throwable, commit: Commit[ValueType]) => ()
  ): CommitAutoPublisher = {
    // Would like a more efficient method that doesn't involve scanning every commit (even defunct ones).
    def publishAll() {
      storage.heads.values foreach {
        commit =>
          try {
            save(commit)
          } catch {
            case e: CommitOrderingException =>
              // Suppress, we'll just try again later
              log.warn(e, "Failed to publish commit, will try again soon: %s", commit.key)
            case e: Throwable =>
              errorHandler(e, commit)
              throw e
          }
      }
    }
    val leaderSelector = new LeaderSelector(
      curator,
      electionPath,
      new LeaderSelectorListenerAdapter
      {
        override def takeLeadership(_curator: CuratorFramework) {
          log.info("Starting automatic commit publishing: period = %s + %s fuzz", period, periodFuzz)
          everyFuzzy(period, periodFuzz, delay) {
            try {
              publishAll()
            } catch {
              case e: Exception =>
                log.warn(e, "Failed to publish commits!")
                throw e
            }
            if (Thread.currentThread().isInterrupted) {
              // Stop doing leader stuff
              throw new InterruptedException("Interrupted!")
            }
          }
        }
      }
    )
    leaderSelector.autoRequeue()
    new CommitAutoPublisher {
      override def start() {
        leaderSelector.start()
      }

      override def close(deadline: Time) = Future {
        leaderSelector.interruptLeadership()
        leaderSelector.close()
      }
    }
  }

  /**
   * Path to the znode for a commit key.
   */
  private def znode(key: String) = ZKPaths.makePath(dataPath, key)

  private def fromKeyAndBytes(key: Commit.Key, bytes: Array[Byte]): Commit[ValueType] = {
    val commit = Commit.deserializeOrThrow[ValueType](bytes)
    require(commit.key == key, "Commit has wrong name: expected '%s', got '%s'" format (key, commit.key))
    commit
  }
}

trait CommitAutoPublisher extends Closable {
  def start(): Unit
  def stop() {
    Await.result(close())
  }
}

class ConcurrentCommitException(msg: String) extends RuntimeException(msg)
