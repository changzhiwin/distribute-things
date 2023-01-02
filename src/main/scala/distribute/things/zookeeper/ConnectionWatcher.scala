package distribute.things.zookeeper

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.{Watcher, WatchedEvent, ZooKeeper, KeeperException}
import org.apache.zookeeper.Watcher.Event.KeeperState
  
class ConnectionWatcher extends Watcher {

  var zk: ZooKeeper = _

  val connectedSignal = new CountDownLatch(1)

  def connect(hosts: String): Unit = {
    zk = new ZooKeeper(hosts, 5000, this)
    connectedSignal.await()
  }

  def close(): Unit = zk.close()

  override def process(event: WatchedEvent): Unit = {
    if (event.getState() == KeeperState.SyncConnected) {
      connectedSignal.countDown()
    }
  }
}