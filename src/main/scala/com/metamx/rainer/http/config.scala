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

import com.metamx.common.scala.db.DBConfig
import com.metamx.common.scala.net.curator.{DiscoAnnounceConfig, DiscoConfig, CuratorConfig}
import org.joda.time.Duration
import org.skife.config.{Default, Config}

object config
{

  trait ServiceConfig
  {
    @Config(Array("com.metamx.rainer.service.name"))
    @Default("rainer:local:ui")
    def serviceName: String

    @Config(Array("com.metamx.rainer.service.host"))
    @Default("localhost")
    def serviceHost: String

    @Config(Array("com.metamx.rainer.service.port"))
    @Default("8080")
    def servicePort: Int

    @Config(Array("com.metamx.rainer.service.threads"))
    @Default("8")
    def threads: Int
  }

  abstract class RainerCuratorConfig extends CuratorConfig with DiscoConfig with ServiceConfig
  {
    @Config(Array("com.metamx.rainer.zk.connect"))
    @Default("localhost")
    override def zkConnect: String

    @Config(Array("com.metamx.rainer.zk.timeout"))
    @Default("PT30S")
    override def zkTimeout: Duration

    @Config(Array("com.metamx.rainer.zk.diaryPath"))
    @Default("/diary-local")
    def diaryPath: String

    @Config(Array("com.metamx.rainer.zk.discoPath"))
    @Default("/disco-local")
    override def discoPath: String

    override def discoAnnounce = Some(new DiscoAnnounceConfig(serviceName, servicePort, false))
  }

  trait RainerDBConfig extends DBConfig
  {
    @Config(Array("com.metamx.rainer.db.uri"))
    @Default("jdbc:mysql://localhost/rainer")
    def uri: String

    @Config(Array("com.metamx.rainer.db.user"))
    @Default("metamx")
    def user: String

    @Config(Array("com.metamx.rainer.db.password"))
    @Default("joshua")
    def password: String

    @Config(Array("com.metamx.rainer.db.queryTimeout"))
    @Default("PT1M")
    def queryTimeout: Duration

    @Config(Array("com.metamx.rainer.db.batchSize"))
    @Default("1000")
    def batchSize: Int

    @Config(Array("com.metamx.rainer.db.fetchSize"))
    @Default("1000")
    def fetchSize: Int
  }

}
