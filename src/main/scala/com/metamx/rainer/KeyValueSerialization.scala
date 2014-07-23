package com.metamx.rainer

import com.fasterxml.jackson.databind.ObjectMapper

trait KeyValueSerialization[ValueType]
{
  def toBytes(key: Commit.Key, value: ValueType): Array[Byte]
}

object KeyValueSerialization
{
  def usingJackson[ValueType: ClassManifest](mapper: ObjectMapper) = new KeyValueSerialization[ValueType] {
    override def toBytes(k: Commit.Key, value: ValueType) = {
      mapper.writeValueAsBytes(value)
    }
  }
}
