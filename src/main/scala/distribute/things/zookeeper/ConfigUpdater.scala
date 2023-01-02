package distribute.things.zookeeper

import scala.util.Random

class ConfigUpdater(hosts: String) {

  val store = new ActiveKeyValueStore()
  store.connect(hosts)

  def run(): Unit = {
    (0 until 10).foreach { x =>
      val value = Random.nextInt(1000) + ""
      store.write("/config", value)
      println(s"Set /config to ${value}")
      Thread.sleep(1000)
    }
  }
}

object ConfigUpdater {

  def main(args: Array[String]): Unit = {
    val configUpdater = new ConfigUpdater("localhost")
    configUpdater.run()
  }

}