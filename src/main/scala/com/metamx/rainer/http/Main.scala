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

import com.metamx.common.config.Config
import com.metamx.common.lifecycle.Lifecycle
import com.metamx.common.lifecycle.Lifecycle.Handler
import com.metamx.common.scala.Predef._
import com.metamx.common.scala.config._
import com.metamx.common.scala.db.MySQLDB
import com.metamx.common.scala.net.curator.{Curator, Disco}
import com.metamx.rainer.http.config.{ServiceConfig, RainerDBConfig, RainerCuratorConfig}
import com.metamx.rainer.{KeyValueDeserialization, CommitStorage, CommitKeeper, DbCommitStorage, DbCommitStorageMySQLMixin}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}
import org.eclipse.jetty.util.thread.QueuedThreadPool

object Main
{

  def main(args: Array[String]) {
    val TableName = "rainer"

    val lifecycle = new Lifecycle
    val configs = Config.createFactory(System.getProperties)
    val serviceConfig = configs.apply[ServiceConfig]
    val curator = Curator.create(configs.apply[RainerCuratorConfig], lifecycle)
    val disco = lifecycle.addManagedInstance(new Disco(curator, configs.apply[RainerCuratorConfig]))
    val db = lifecycle.addManagedInstance(new MySQLDB(configs.apply[RainerDBConfig]) with DbCommitStorageMySQLMixin)
    val storage = new DbCommitStorage[DictValue](db, TableName)
    val keeper = new CommitKeeper[DictValue](curator, configs.apply[RainerCuratorConfig].diaryPath)
    val servlet = new RainerServlet[DictValue] {
      override def valueDeserialization = implicitly[KeyValueDeserialization[DictValue]]
      override def commitStorage = CommitStorage.keeperPublishing(storage, keeper)
    }
    val server = new Server
    server.setConnectors(
      Array(
        new SelectChannelConnector withEffect {
          connector =>
            connector.setPort(serviceConfig.servicePort)
        }
      )
    )
    server.setThreadPool(new QueuedThreadPool(serviceConfig.threads))
    server.setHandler(
      new ServletContextHandler(ServletContextHandler.NO_SESSIONS) withEffect {
        context =>
          context.setContextPath("/")
          context.addServlet(new ServletHolder(servlet), "/diary/*")
      }
    )
    lifecycle.addHandler(
      new Handler
      {
        override def start() {
          server.start()
        }

        override def stop() {
          server.stop()
        }
      }
    )
    lifecycle.start()
    try {
      lifecycle.join()
    }
    finally {
      lifecycle.stop()
    }
  }
}
