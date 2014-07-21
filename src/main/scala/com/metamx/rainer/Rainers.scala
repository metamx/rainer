package com.metamx.rainer

import com.metamx.common.lifecycle.{LifecycleStart, LifecycleStop}
import org.apache.curator.framework.CuratorFramework
import org.scala_tools.time.Imports._

class Rainers[ValueType: KeyValueDeserialization](
  val storage: CommitStorage[ValueType],
  val keeper: CommitKeeper[ValueType],
  autoPublisher: CommitAutoPublisher
)
{
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
