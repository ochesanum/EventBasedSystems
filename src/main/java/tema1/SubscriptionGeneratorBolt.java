package tema1;

import java.util.*;

import lombok.RequiredArgsConstructor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@RequiredArgsConstructor
public class SubscriptionGeneratorBolt extends BaseRichBolt {
    private final int totalSubscriptions;
    private final double stationIdFrequency;
    private final double cityFrequency;
    private final double tempFrequency;
    private final double rainFrequency;
    private final double windFrequency;
    private final double directionFrequency;
    private final double dateFrequency;

    private Random random;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.random = new Random();
    }

    @Override
    public void execute(Tuple tuple) {
        // Read the publication tuple
        int stationid = tuple.getIntegerByField("stationid");
        String city = tuple.getStringByField("city");
        int temp = tuple.getIntegerByField("temp");
        double rain = tuple.getDoubleByField("rain");
        int wind = tuple.getIntegerByField("wind");
        String direction = tuple.getStringByField("direction");
        String date = tuple.getStringByField("date");

        // Generate subscriptions based on the frequency of the data
        for (int i = 0; i < totalSubscriptions; i++) {
            Values values = new Values();
            // Add fields based on their frequency
            if (random.nextDouble() < stationIdFrequency) {
                values.add(stationid);
            }
            if (random.nextDouble() < cityFrequency) {
                values.add(city);
            }
            if (random.nextDouble() < tempFrequency) {
                values.add(temp);
            }
            if (random.nextDouble() < rainFrequency) {
                values.add(rain);
            }
            if (random.nextDouble() < windFrequency) {
                values.add(wind);
            }
            if (random.nextDouble() < directionFrequency) {
                values.add(direction);
            }
            if (random.nextDouble() < dateFrequency) {
                values.add(date);
            }
            collector.emit(values);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stationid","city", "temp", "rain", "wind", "direction", "date"));
    }

}
