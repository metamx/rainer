package com.metamx.rainer

import com.github.nscala_time.time.Imports._
import com.metamx.common.lifecycle.{LifecycleStart, LifecycleStop}
import com.metamx.rainer.http.RainerServlet
import org.apache.curator.framework.CuratorFramework

class Rainers[ValueType: KeyValueDeserialization](
  val storage: CommitStorage[ValueType],
  val keeper: CommitKeeper[ValueType],
  autoPublisher: CommitAutoPublisher
)
{
  lazy val servlet = new RainerServlet[ValueType] {
    override def commitStorage = storage

    override def valueDeserialization = implicitly[KeyValueDeserialization[ValueType]]
  }

  @LifecycleStart
  def start() {
    storage.start()
    autoPublisher.start()
  }

  @LifecycleStop
  def stop() {
    autoPublisher.stop()
    storage.stop()
  }
}

object Rainers
{
  def create[ValueType: KeyValueDeserialization](
    curator: CuratorFramework,
    zkPath: String,
    underlyingStorage: CommitStorage[ValueType],
    autoPublishingDuration: Duration = 90.seconds,
    autoPublishingFuzz: Double = 0.2,
    autoPublishingDelay: Boolean = true,
    autoPublishingErrorHandler: (Throwable, Commit[ValueType]) => Unit = (e: Throwable, commit: Commit[ValueType]) => ()
  ): Rainers[ValueType] =
  {
    val keeper = new CommitKeeper[ValueType](curator, zkPath)
    val linkedStorage = CommitStorage.keeperPublishing(underlyingStorage, keeper)
    val autoPublisher = keeper.autoPublisher(
      underlyingStorage,
      autoPublishingDuration,
      autoPublishingFuzz,
      autoPublishingDelay,
      autoPublishingErrorHandler
    )
    new Rainers[ValueType](linkedStorage, keeper, autoPublisher)
  }
}
