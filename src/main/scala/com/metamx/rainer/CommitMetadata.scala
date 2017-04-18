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
import com.metamx.common.scala.untyped._

/**
 * Metadata for a key/value commit. Everything but the payload.
 *
 * @param key Key for this commit
 * @param version Version of this commit (increases by 1 for each commit)
 * @param author Author for this commit. Can be any string you like!
 * @param comment Comment for this commit. Can also be any string you like!
 * @param mtime Timestamp for this commit. Should ideally be the time the commit was generated.
 * @param empty Whether or not this commit has a payload. Empty commits can be used as tombstones to
 *              represent deletions.
 */
case class CommitMetadata(
  key: Commit.Key,
  version: Int,
  author: Commit.Author,
  comment: Commit.Comment,
  mtime: DateTime,
  empty: Boolean
)
{
  require(key.nonEmpty, "Key must be nonempty")

  def asMap: Dict = Map(
    "key" -> key,
    "version" -> version,
    "author" -> author,
    "comment" -> comment,
    "mtime" -> mtime.toString(),
    "empty" -> empty
  )
}

object CommitMetadata
{
  def fromMap(d: Dict): CommitMetadata = {
    val empty = bool(d.getOrElse("empty", false))
    CommitMetadata(
      str(d("key")),
      int(d("version")),
      str(d("author")),
      str(d("comment")),
      new DateTime(str(d("mtime"))),
      empty
    )
  }
}
