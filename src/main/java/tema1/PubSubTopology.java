package tema1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import tema1.pubsub.PublicationSpout;
import tema1.pubsub.TerminalBolt;

public class PubSubTopology {

    public static void main(String[] args) throws InterruptedException {
        int numMessages = 4;
        double stationIdFreq =1.0;
        double cityFreq = 1.0;
        double tempFreq = 1.0;
        double rainFreq = 1.0;
        double windFreq = 1.0;
        double directionFreq =1.0;
        double dateFreq = 1.0;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("pub-spout", new PublicationSpout(numMessages),2);
        builder.setBolt("sub-bolt", new SubscriptionGeneratorBolt(1, stationIdFreq,cityFreq, tempFreq, rainFreq, windFreq, directionFreq, dateFreq),2).setNumTasks(4)
                .allGrouping("pub-spout");
        builder.setBolt("terminal-bolt", new TerminalBolt(),2).globalGrouping("sub-bolt");

        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,1024);
        config.put(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE,1);
        config.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("pub-sub-topology", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("pub-sub-topology");
        cluster.shutdown();
    }
}

