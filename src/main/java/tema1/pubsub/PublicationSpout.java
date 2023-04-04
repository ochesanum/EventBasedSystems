package tema1.pubsub;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.BiFunction;

import static tema1.pubsub.Constants.*;

public class PublicationSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final int totalPublications;
    private int currentPublication;
    private final List<String> cities = Arrays.asList("Iasi", "Bucuresti", "Vaslui", "Suceava", "Todirel", "下北沢");
    private final List<String> directions = Arrays.asList("N", "NE", "E", "SE", "S", "SW", "W", "NW");
    private final List<String> stationIds = Arrays.asList("1","2","3","4","5","6","7","8","9","10");
    private Random random;

    public PublicationSpout(int totalPublications) {
        this.totalPublications = totalPublications;
        this.currentPublication = 0;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        if (this.currentPublication < this.totalPublications) {
            String stationId = this.stationIds.get(this.random.nextInt(this.stationIds.size()));
            String city = this.cities.get(this.random.nextInt(this.cities.size()));
            int temp = this.random.nextInt(30);
            double rain = this.random.nextDouble();
            int wind = this.random.nextInt(100);
            String direction = this.directions.get(this.random.nextInt(this.directions.size()));
            String date = new SimpleDateFormat("dd.MM.yyyy").format(new Date());

            Values values = new Values(stationId, city, temp, rain, wind, direction, date);
            this.collector.emit(values);

            this.currentPublication++;
        } else {
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(STATION_ID, CITY, TEMPERATURE, RAIN, WIND, DIRECTION, DATE));
    }
}
