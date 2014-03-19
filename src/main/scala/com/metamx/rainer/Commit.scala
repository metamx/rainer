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

import com.google.common.base.Charsets
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.exception._
import com.metamx.common.scala.untyped._
import org.scala_tools.time.Imports._

/**
 * Represents a key/value commit.
 *
 * @param key Key for this commit
 * @param version Version of this commit (increases by 1 for each commit)
 * @param payload Serialized value for this commit, or None if this is a tombstone commit.
 * @param author Author for this commit. Can be any string you like!
 * @param comment Comment for this commit. Can also be any string you like!
 * @param mtime Timestamp for this commit. Should ideally be the time the commit was generated.
 * @tparam ValueType Underlying object type being committed. Should really be immutable and possess semantic equals
 *                   and hashCode methods.
 */
class Commit[ValueType: KeyValueDeserialization](
  val key: Commit.Key,
  val version: Int,
  val payload: Option[Array[Byte]],
  val author: Commit.Author,
  val comment: Commit.Comment,
  val mtime: DateTime
) extends Equals
{
  require(key.nonEmpty, "key must be nonempty")

  def value: Option[Either[Exception, ValueType]] = payload map {
    bytes =>
      implicitly[KeyValueDeserialization[ValueType]].fromKeyAndBytes(key, bytes).catchEither[Exception]
  }

  def metadata: Dict = Map(
    "key" -> key,
    "version" -> version,
    "author" -> author,
    "comment" -> comment,
    "mtime" -> mtime.toString(),
    "empty" -> payload.isEmpty
  )

  def canEqual(other: Any): Boolean = other.isInstanceOf[Commit[_]]

  override def equals(other: Any): Boolean = other match {
    case that: Commit[_] =>
      (that canEqual this) &&
        key == that.key &&
        version == that.version &&
        ((payload.isEmpty && that.payload.isEmpty)
          || (payload.nonEmpty && that.payload.nonEmpty && payload.get.deep == that.payload.get.deep)) &&
        author == that.author &&
        comment == that.comment &&
        mtime == that.mtime
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(key, version, payload.getOrElse(Array.empty).deep, author, comment, mtime)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = {
    val payloadString = payload.map("[%,d bytes]" format _.size).getOrElse("[empty]")
    "Commit%s" format ((key, version, payloadString, author, comment, mtime))
  }
}

object Commit
{
  type Key = String
  type Author = String
  type Comment = String

  def create[ValueType: KeyValueDeserialization](
    key: Commit.Key,
    version: Int,
    payload: Option[Array[Byte]],
    author: Commit.Author,
    comment: Commit.Comment,
    mtime: DateTime
  ): Commit[ValueType] = new Commit(
    key,
    version,
    payload,
    author,
    comment,
    mtime
  )

  def serialize[ValueType](commit: Commit[ValueType]): Array[Byte] = {
    val headerBytes = Jackson.bytes(commit.metadata)
    val valueBytes = commit.payload.getOrElse(Array.empty)
    val newLine = Array[Byte]('\n')
    headerBytes.size.toString.getBytes(Charsets.UTF_8) ++ newLine ++ headerBytes ++ newLine ++ valueBytes ++ newLine
  }

  def deserializeOrThrow[ValueType : KeyValueDeserialization](bytes: Array[Byte]): Commit[ValueType] = {
    val headerSizeBytes = bytes.takeWhile(_ != '\n')
    val headerSize = new String(headerSizeBytes).toInt
    val headerBytes = bytes.drop(headerSizeBytes.size + 1).take(headerSize)
    val valueBytes = bytes.drop(headerSizeBytes.size + 1 + headerSize + 1).dropRight(1)
    val header = Jackson.parse[Dict](headerBytes)
    fromMetadataAndPayload(header, valueBytes)
  }

  def fromMetadataAndPayload[ValueType: KeyValueDeserialization](
    meta: Dict,
    payload: Array[Byte]
  ): Commit[ValueType] =
  {
    val empty = bool(meta.getOrElse("empty", false))
    new Commit(
      str(meta("key")),
      int(meta("version")),
      if (empty) None else Some(payload),
      str(meta("author")),
      str(meta("comment")),
      new DateTime(str(meta("mtime")))
    )
  }
}
