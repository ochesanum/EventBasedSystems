package tema1.pubsub;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import tema1.pubsub.models.Publication;
import tema1.pubsub.models.Subscription;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class PubSubBolt extends BaseRichBolt {
    private OutputCollector collector;

    private List<Subscription> subscriptions;

    private List<Publication> publications;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.publications = new ArrayList<>();
        this.subscriptions = new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals("pub-spout")) {
            publications.add((Publication) tuple.getValueByField("publication"));
        }
        else{
            subscriptions.add((Subscription) tuple.getValueByField("subscription"));
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup(){
        try {
            Random random = new Random();
            FileWriter myWriter = new FileWriter("output" + String.valueOf(random.nextInt()) + ".txt");
            for(Publication publication : publications)
                myWriter.write(String.valueOf(publication) + "\n");
            myWriter.write("\n");
            for(Subscription subscription : subscriptions)
                myWriter.write(String.valueOf(subscription) + "\n");
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
