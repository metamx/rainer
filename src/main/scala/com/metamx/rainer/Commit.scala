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
import com.google.common.base.Charsets
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.exception._
import com.metamx.common.scala.untyped._

/**
 * Represents a key/value commit. The value is stored as a byte array, although Commits always know how to
 * deserialize them into a useful ValueType, using a KeyValueDeserialization.
 *
 * @param meta Metadata for this comment.
 * @param payload Serialized value for this commit, or None if this is a tombstone commit.
 * @tparam ValueType Underlying object type being committed. Should really be immutable and possess semantic equals
 *                   and hashCode methods.
 */
class Commit[ValueType: KeyValueDeserialization](
  val meta: CommitMetadata,
  val payload: Option[Array[Byte]]
) extends Equals
{
  require(
    (meta.empty && payload.isEmpty) || (!meta.empty && !payload.isEmpty),
    "meta.empty and payload.isEmpty must match"
  )

  def key = meta.key
  def version = meta.version
  def author = meta.author
  def comment = meta.comment
  def mtime = meta.mtime
  def isEmpty = payload.isEmpty

  /**
   * Deserialized value for this commit. This is an Option (because payloads can be present, or not present) of an
   * Either (because deserialization can succeed, or fail). If you are uninterested in this distinction, you can use
   * the simpler "valueOption" method.
   */
  def value: Option[Either[Exception, ValueType]] = payload map {
    bytes =>
      implicitly[KeyValueDeserialization[ValueType]].fromKeyAndBytes(key, bytes).catchEither[Exception]
  }

  /**
   * Deserialized value for this commit. This waves away the nuance of the "value" method, and represents
   * missing payloads and failed deserializations both as Options. If you need to be able to distinguish between
   * those two cases, or if you need access to the actual exception that was thrown, you should use "value".
   */
  def valueOption: Option[ValueType] = value.flatMap(_.right.toOption)

  def canEqual(other: Any): Boolean = other.isInstanceOf[Commit[_]]

  override def equals(other: Any): Boolean = other match {
    case that: Commit[_] =>
      (that canEqual this) &&
        key == that.key &&
        version == that.version &&
        ((payload.isEmpty && that.isEmpty)
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

  def apply[ValueType: KeyValueDeserialization](
    meta: CommitMetadata,
    payload: Option[Array[Byte]]
  ): Commit[ValueType] =
  {
    new Commit(meta, payload)
  }

  def apply[ValueType: KeyValueDeserialization](
    key: Commit.Key,
    version: Int,
    payload: Option[Array[Byte]],
    author: Commit.Author,
    comment: Commit.Comment,
    mtime: DateTime
  ): Commit[ValueType] = {
    fromBytes(key, version, payload, author, comment, mtime)
  }

  def fromBytes[ValueType: KeyValueDeserialization](
    key: Commit.Key,
    version: Int,
    payload: Option[Array[Byte]],
    author: Commit.Author,
    comment: Commit.Comment,
    mtime: DateTime
  ): Commit[ValueType] = {
    Commit(
      CommitMetadata(key, version, author, comment, mtime, payload.isEmpty),
      payload
    )
  }

  def fromValue[ValueType: KeyValueDeserialization: KeyValueSerialization](
    key: Commit.Key,
    version: Int,
    payload: Option[ValueType],
    author: Commit.Author,
    comment: Commit.Comment,
    mtime: DateTime
  ): Commit[ValueType] = {
    Commit(
      CommitMetadata(key, version, author, comment, mtime, payload.isEmpty),
      payload map (implicitly[KeyValueSerialization[ValueType]].toBytes(key, _))
    )
  }

  def unapply[ValueType](commit: Commit[ValueType]): Option[(CommitMetadata, Option[Either[Exception, ValueType]])] = {
    Some((commit.meta, commit.value))
  }

  def serialize[ValueType](commit: Commit[ValueType]): Array[Byte] = {
    val headerBytes = Jackson.bytes(commit.meta.asMap)
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
    val meta = CommitMetadata.fromMap(header)
    Commit(meta, if (meta.empty) None else Some(valueBytes))
  }
}
