package distribute.things.zookeeper

import org.apache.zookeeper.{Watcher, WatchedEvent, ZooKeeper, CreateMode, KeeperException}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher.Event.EventType

class ConfigWatcher(hosts: String) extends Watcher {

  val store = new ActiveKeyValueStore()
  store.connect(hosts)

  def displayConfig(): Unit = {
    val value = store.read("/config", this)
    println(s"Read /config as ${value}")
  }

  override def process(event: WatchedEvent): Unit = {
    if (event.getType == EventType.NodeDataChanged) {
      try {
        displayConfig()
      } catch {
        case e1: InterruptedException => {
          println("Interrupted. Exiting.")
        }
        case e2: KeeperException => {
          println(s"KeeperException: ${e2}. Exiting.")
        }
      }
    }
  }
}

object ConfigWatcher {
  def main(args: Array[String]): Unit = {
    val configWatcher = new ConfigWatcher("localhost")
    configWatcher.displayConfig()

    Thread.sleep(Long.MaxValue)
  }
}