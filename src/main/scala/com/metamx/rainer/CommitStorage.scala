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

import com.metamx.common.scala.Logging

/**
 * Long-term storage for full commit history.
 */
trait CommitStorage[ValueType]
{
  /**
   * Acquire or set up any necessary resources. Behavior of other methods is undefined before start() is called.
   */
  def start()

  /**
   * Release all held resources. Behavior of other methods is undefined after stop() is called.
   */
  def stop()

  /**
   * Returns all current commits (highest version for each key).
   */
  def heads: Map[Commit.Key, Commit[ValueType]]

  /**
   * Returns all current commits (highest version for each key) that are also non-empty.
   */
  def headsNonEmpty: Map[Commit.Key, Commit[ValueType]]

  /**
   * Get the current commit (highest version) for a particular key.
   */
  def get(key: Commit.Key): Option[Commit[ValueType]]

  /**
   * Get a particular commit for a particular key.
   */
  def get(key: Commit.Key, version: Int): Option[Commit[ValueType]]

  /**
   * Saves a new commit. The version should be one higher than the previous commit's version for the same key. This
   * method will throw a {{{CommitOrderingException}}} if the storage implementation detects violation of that rule.
   */
  def save(commit: Commit[ValueType])
}

object CommitStorage extends Logging
{
  def keeperPublishing[ValueType](
    delegate: CommitStorage[ValueType],
    commitKeeper: CommitKeeper[ValueType]
  ) = {
    CommitStorage.withPostSaveHook(delegate) {
      commit =>
        // Push commit to ZK immediately
        try commitKeeper.save(commit)
        catch {
          case e: Exception =>
            // We can suppress errors here, since we've already committed the commit to the database, and the
            // autoPublisher hopefully running somewhere will sync them to ZK eventually.
            log.warn(
              e, "Suppressed exception while trying to publish commit[%s] to ZooKeeper. "
                + "I hope you have an autoPublisher somewhere!", commit.key
            )
        }
    }
  }

  def withPreSaveHook[ValueType](delegate: CommitStorage[ValueType])(hook: Commit[ValueType] => Unit) = {
    new CommitStorage[ValueType]
    {
      override def start() {
        delegate.start()
      }

      override def stop() {
        delegate.stop()
      }

      override def save(commit: Commit[ValueType]) {
        hook(commit)
        delegate.save(commit)
      }

      override def get(key: Commit.Key, version: Int) = delegate.get(key, version)

      override def get(key: Commit.Key) = delegate.get(key)

      override def heads = delegate.heads

      override def headsNonEmpty = delegate.headsNonEmpty
    }
  }

  def withPostSaveHook[ValueType](delegate: CommitStorage[ValueType])(hook: Commit[ValueType] => Unit) = {
    new CommitStorage[ValueType]
    {
      override def start() {
        delegate.start()
      }

      override def stop() {
        delegate.stop()
      }

      override def save(commit: Commit[ValueType]) {
        delegate.save(commit)
        hook(commit)
      }

      override def get(key: Commit.Key, version: Int) = delegate.get(key, version)

      override def get(key: Commit.Key) = delegate.get(key)

      override def heads = delegate.heads

      override def headsNonEmpty = delegate.headsNonEmpty
    }
  }
}
