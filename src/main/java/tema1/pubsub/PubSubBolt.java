package tema1.pubsub;

import lombok.var;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import tema1.pubsub.models.Publication;
import tema1.pubsub.models.Subscription;
import tema1.pubsub.models.SubscriptionEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PubSubBolt extends BaseRichBolt {
    private OutputCollector collector;

    private List<Subscription> subscriptions;

    private List<Publication> publications;
    int count = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.publications = new ArrayList<>();
        this.subscriptions = new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceComponent().equals("pub-spout")) {
            publications.add((Publication) tuple.getValueByField("publication"));
        } else {
            subscriptions.add((Subscription) tuple.getValueByField("subscription"));
        }
        collector.ack(tuple);
    }

    public boolean matchSubscriptionEntryWithPublication(SubscriptionEntry subscriptionEntry, Publication publication) {
        if (subscriptionEntry.getField().equals("city")) {
            if (subscriptionEntry.getValue().equals(publication.getCity()))
                return true;
        }
        if (subscriptionEntry.getField().equals("date")) {
            return publication.getDate().equals((String) subscriptionEntry.getValue());
        }
        if (subscriptionEntry.getField().equals("stationid")) {
            return subscriptionEntry.getValue().equals(publication.getStationId());
        }
        if (subscriptionEntry.getField().equals("direction"))
            return subscriptionEntry.getValue().equals(publication.getDirection());
        if (subscriptionEntry.getField().equals("rain")) {
            if (subscriptionEntry.getValue().equals(publication.getRain()) && subscriptionEntry.getOperator().equals("=="))
                return true;
            if ((float) subscriptionEntry.getValue() <= publication.getRain() && (subscriptionEntry.getOperator().equals("<=") || subscriptionEntry.getOperator().equals("<")))
                return true;
            if ((float) subscriptionEntry.getValue() >= publication.getRain() && (subscriptionEntry.getOperator().equals(">=") || subscriptionEntry.getOperator().equals(">")))
                return true;
        }
        if (subscriptionEntry.getField().equals("temp")) {
            if (subscriptionEntry.getValue().equals(publication.getTemp()) && subscriptionEntry.getOperator().equals("=="))
                return true;
            if ((int) subscriptionEntry.getValue() <= publication.getTemp() && (subscriptionEntry.getOperator().equals("<=") || subscriptionEntry.getOperator().equals("<")))
                return true;
            if ((int) subscriptionEntry.getValue() >= publication.getTemp() && (subscriptionEntry.getOperator().equals(">=") || subscriptionEntry.getOperator().equals(">")))
                return true;
        }
        if (subscriptionEntry.getField().equals("wind")) {
            if (subscriptionEntry.getValue().equals(publication.getWind()) && subscriptionEntry.getOperator().equals("=="))
                return true;
            if ((int) subscriptionEntry.getValue() <= publication.getWind() && (subscriptionEntry.getOperator().equals("<=") || subscriptionEntry.getOperator().equals("<")))
                return true;
            if ((int) subscriptionEntry.getValue() >= publication.getWind() && (subscriptionEntry.getOperator().equals(">=") || subscriptionEntry.getOperator().equals(">")))
                return true;
        }
        return false;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        for (Subscription subscription : subscriptions) {
            if (!(subscription == null))
                for (Publication publication : publications) {
                    var entries = subscription.getEntries();
                    boolean isMatched = true;
                    for (SubscriptionEntry subscriptionEntry : entries) {
                        if (!matchSubscriptionEntryWithPublication(subscriptionEntry, publication))
                            isMatched = false;
                    }
                    if (isMatched) {
                        count++;
                        System.out.println("Match found: " + subscription.toString() + " " + publication.toString());
                    }
                }
        }
        System.out.println("Count: " + count);
    }
}
