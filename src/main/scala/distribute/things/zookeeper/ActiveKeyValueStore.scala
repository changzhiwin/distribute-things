package distribute.things.zookeeper

import java.nio.charset.Charset
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.{Watcher, WatchedEvent, ZooKeeper, CreateMode, KeeperException}
import org.apache.zookeeper.ZooDefs.Ids

class ActiveKeyValueStore extends ConnectionWatcher {

  lazy val CharSET = Charset.forName("UTF-8")

  def write(path: String, value: String): Unit = {
    val statOpt = Option(zk.exists(path, false))
    
    statOpt match {
      case Some(stat) =>
        zk.setData(path, value.getBytes(CharSET), -1)
      case None       =>
        zk.create(path, value.getBytes(CharSET), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
  }

  def read(path: String, watcher: Watcher): String = {
    val data = zk.getData(path, watcher, null)
    return new String(data, CharSET)
  }
}