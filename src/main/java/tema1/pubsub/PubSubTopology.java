package tema1.pubsub;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import tema1.pubsub.models.Publication;
import tema1.pubsub.models.Subscription;
import tema1.pubsub.models.SubscriptionEntry;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

import static tema1.pubsub.utils.Constants.*;

public class PubSubTopology {
    public static void main(String[] args) throws InterruptedException {
        Instant inst1 = Instant.now();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sub-spout", new SubscriptionSpout(10000,new HashMap<String,Float>(){{
            put(STATION_ID, 0.92f);
            put(CITY, 0.1f);
            put(TEMPERATURE, 0.23f);
            put(RAIN, 0.49f);
            put(WIND, 0.12f);
            put(DIRECTION, 0.4426f);
            put(DATE, 0.4f);
        }}),5).setNumTasks(1000);
        builder.setSpout("pub-spout", new PublicationSpout(10000),5).setNumTasks(1000);
        builder.setBolt("pubsub-bolt", new PubSubBolt(),5).allGrouping("sub-spout").allGrouping("pub-spout").setNumTasks(1);

        Config config = new Config();
        config.registerSerialization(Subscription.class);
        config.registerSerialization(SubscriptionEntry.class);
        config.registerSerialization(Publication.class);
        config.setDebug(true);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,1024);
        config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE,1);
        config.setNumWorkers(20);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("pub-sub-topology", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("pub-sub-topology");
        cluster.shutdown();
        Instant inst2 = Instant.now();
        System.out.println("Elapsed Time: "+ Duration.between(inst1, inst2).toString());
    }


}
