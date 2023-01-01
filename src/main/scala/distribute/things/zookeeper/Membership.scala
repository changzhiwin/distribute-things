package distribute.things.zookeeper

import java.util.concurrent.CountDownLatch;

import scala.jdk.CollectionConverters._

import org.apache.zookeeper.{Watcher, WatchedEvent, ZooKeeper, CreateMode, KeeperException}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.ZooDefs.Ids

object Membership {

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

  class GroupOperator extends ConnectionWatcher {
    
    def create(groupName: String): Unit = {
      val path = "/" + groupName
      val createPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      println(s"Created: [${createPath}]")
    }

    def join(groupName: String, memberName: String): Unit = {
      val path = s"/${groupName}/${memberName}"
      val createPath = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
      println(s"Joined: [${createPath}]")
    }

    def list(groupName: String): Unit = {
      val path = s"/${groupName}"
      try {
        val children = zk.getChildren(path, false).asScala
        if (children.isEmpty) {
          println(s"No members in group ${groupName}")
          System.exit(1)
        }

        for (child <- children) {
          println(s"In [${groupName}]: [${child}]")
        }
      } catch {
        case e: KeeperException.NoNodeException =>
          println(s"Group ${groupName} does not exist")
          System.exit(1)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val group = new GroupOperator()
    group.connect("localhost")
    group.join("zoo", "duck")
    group.list("zoo")
    group.close()
    //Thread.sleep(Long.MaxValue)
  }
}

// Ref: https://zookeeper.apache.org/doc/r3.7.1/apidocs/zookeeper-server/index.html