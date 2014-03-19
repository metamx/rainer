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

package com.metamx.rainer.http

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.untyped.Dict
import com.metamx.rainer.KeyValueDeserialization

case class DictValue(s: String)
{
  val dict = DictValue.jsonMapper.readValue(s, classOf[Dict])
}

object DictValue
{
  private val jsonMapper = new ObjectMapper() withEffect {
    jm =>
      jm.registerModule(DefaultScalaModule)
  }

  implicit val deserialization = new KeyValueDeserialization[DictValue] {
    override def fromKeyAndBytes(k: String, bytes: Array[Byte]) = DictValue(new String(bytes, Charsets.UTF_8))
  }
}
