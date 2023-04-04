package tema1.pubsub;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import tema1.pubsub.Publication;

import java.util.Map;

public class TerminalBolt extends BaseRichBolt {

    private Publication publication;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.publication = new Publication();
    }

    @Override
    public void execute(Tuple tuple) {
        int stationid = tuple.getIntegerByField("stationid");
        String city = tuple.getStringByField("city");
        int temp = tuple.getIntegerByField("temp");
        double rain = tuple.getDoubleByField("rain");
        int wind = tuple.getIntegerByField("wind");
        String direction = tuple.getStringByField("direction");
        String date = tuple.getStringByField("date");
        publication.setStationId(stationid);
        publication.setCity(city);
        publication.setTemp(temp);
        publication.setRain(rain);
        publication.setWind(wind);
        publication.setDirection(direction);
        publication.setDate(date);
        System.out.println(publication);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output fields
    }

}

