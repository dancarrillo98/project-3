package scala

import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions, NewTopic}
import scala.collection.JavaConverters._
import java.util.Properties

object Topic{

    def main(args: Array[String]): Unit = {
        val kafkaServer = "sandbox-hdp.hortonworks.com: 6667"
        val topic = "test6"

        val kafkaConnect = kafkaServer
        val sessionTimeoutMs = 10 * 1000
        val connectionTimeoutMs = 8 * 1000

        val partitions = 1
        val replication:Short = 1
        val topicConfig = new Properties() // add per-topic configurations settings here

        import org.apache.kafka.clients.admin.AdminClientConfig
        val config = new Properties()
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
        val admin = AdminClient.create(config)

        val newTopic = new NewTopic(topic, partitions, replication)
        newTopic.configs(Map[String,String]().asJava)
        val ret = admin.createTopics(List(newTopic).asJavaCollection)
        val ret2 = ret.values().get(topic)
        println(ret2.get())

        val existing = admin.listTopics(new ListTopicsOptions().timeoutMs(5000).listInternal(true))
        val nms = existing.namesToListings()
        nms.get().asScala.foreach(nm => println(nm))

        admin.close()
    }

}