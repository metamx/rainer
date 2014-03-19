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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.metamx.common.scala.Predef._

case class TestPayloadStrict(s: String)
{
  require(s.length % 2 == 0, "string length must be even")
}

object TestPayloadStrict
{
  val jsonMapper = new ObjectMapper() withEffect {
    jm =>
      jm.registerModule(DefaultScalaModule)
  }

  implicit val bytesSerialization = KeyValueDeserialization.usingJackson[TestPayloadStrict](jsonMapper)
}
