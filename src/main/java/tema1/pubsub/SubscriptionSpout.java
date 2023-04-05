package tema1.pubsub;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import tema1.pubsub.models.Subscription;
import tema1.pubsub.models.SubscriptionEntry;

import java.util.*;

import static tema1.pubsub.utils.Constants.*;

public class SubscriptionSpout extends BaseRichSpout {
    private final int totalSubscriptions;
    private SpoutOutputCollector collector;
    private int sentSubscriptions;
    private Map<String, Float> fieldWeights;
    private Map<String, Integer> fieldNumbers;
    private List<Subscription> subscriptions;
    private int subscriptionIndex;

    private final List<String> stationIds = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    private final List<String> dates = Arrays.asList("01.04.2023", "02.04.2023", "03.04.2023", "04.04.2023");
    private final List<String> cities = Arrays.asList("Iasi", "Bucuresti", "Vaslui", "Suceava", "Todirel", "下北沢");
    private final List<String> directions = Arrays.asList("N", "NE", "E", "SE", "S", "SW", "W", "NW");

    public SubscriptionSpout(int totalSubscriptions, Map<String, Float> fieldWeights) {
        this.totalSubscriptions = totalSubscriptions;
        this.sentSubscriptions = 0;
        this.fieldWeights = new HashMap<>(fieldWeights);
        this.fieldNumbers = new HashMap<>();

        for (Map.Entry<String, Float> entry : fieldWeights.entrySet()) {
            fieldNumbers.put(entry.getKey(), (int) (totalSubscriptions * entry.getValue()));
        }

        this.subscriptions = new ArrayList<>();
        generateSubscriptionEntries();
    }

    // stationid, city, temp, rain, wind, direction, date
    private List<Subscription> generateSubscriptionEntries() {
        Random random = new Random();

        for (int i = 0; i < totalSubscriptions; i++)
            subscriptions.add(new Subscription());

        for (Map.Entry<String, Integer> entry : fieldNumbers.entrySet()) {
            int lowerLimitInt = 69, upperLimitInt = 69;
            float lowerLimitFloat = 0.1f, upperLimitFloat = 0.1f;

            List<String> entrySet = new ArrayList<>();
            String field = entry.getKey();

            switch (field) {
                case "city":
                    entrySet = cities;
                    break;
                case "direction":
                    entrySet = directions;
                    break;
                case "stationid":
                    entrySet = stationIds;
                    break;
                case "date":
                    entrySet = dates;
                    break;
                case "temperature":
                    lowerLimitFloat = -20f;
                    upperLimitFloat = 40f;
                    break;
                case "rain":
                    lowerLimitFloat = 0f;
                    upperLimitFloat = 10f;
                    break;
                case "wind":
                    lowerLimitInt = 0;
                    upperLimitInt = 97;
                    break;
                default:
                    System.out.println("kys");

            }
            if (field.equals("city") || field.equals("direction") || field.equals("stationid") || field.equals("date"))
                while (entry.getValue() > 0) {
                    Subscription subscription = subscriptions.get(subscriptionIndex);

                    String value = entrySet.get(random.nextInt(entrySet.size()));
                    String operator = "=";

                    subscription.getEntries().add(new SubscriptionEntry<>(field, operator, value));


                    entry.setValue(entry.getValue() - 1);
                    incrementSubscriptionIndex();
                }
            else if (field.equals("temperature") || field.equals("rain")) {
                while (entry.getValue() > 0) {
                    Subscription subscription = subscriptions.get(subscriptionIndex);
                    float value = (float) (lowerLimitFloat + Math.random() * (upperLimitFloat - lowerLimitFloat));
                    int operator = random.nextInt(5);
                    String sOperator = "";

                    switch (operator) {
                        case 0:
                            sOperator = ">";
                            break;
                        case 1:
                            sOperator = ">=";
                            break;
                        case 2:
                            sOperator = "<";
                            break;
                        case 3:
                            sOperator = "<=";
                            break;
                        case 4:
                            sOperator = "=";
                            break;
                        default:
                            System.out.println("kys fast");
                            break;
                    }
                    subscription.getEntries().add(new SubscriptionEntry<>(field, sOperator, value));

                    entry.setValue(entry.getValue() - 1);
                    incrementSubscriptionIndex();
                }
            } else if (field.equals("wind")) {
                while (entry.getValue() > 0) {
                    Subscription subscription = subscriptions.get(subscriptionIndex);
                    int value = (int) (lowerLimitInt + Math.random() * (upperLimitInt - lowerLimitInt));

                    int operator = random.nextInt(5);
                    String sOperator = "";

                    switch (operator) {
                        case 0:
                            sOperator = ">";
                            break;
                        case 1:
                            sOperator = ">=";
                            break;
                        case 2:
                            sOperator = "<";
                            break;
                        case 3:
                            sOperator = "<=";
                            break;
                        case 4:
                            sOperator = "=";
                            break;
                        default:
                            System.out.println("kys fast (int)");
                            break;
                    }

                    subscription.getEntries().add(new SubscriptionEntry<>(field, sOperator, value));

                    entry.setValue(entry.getValue() - 1);
                    incrementSubscriptionIndex();
                }
            }
        }

        return subscriptions;
    }

    private void incrementSubscriptionIndex() {
        if (this.subscriptionIndex == subscriptions.size() - 1) {
            this.subscriptionIndex = 0;
            return;
        }
        this.subscriptionIndex++;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (this.sentSubscriptions < this.totalSubscriptions) {
            Values values = new Values(subscriptions.get(sentSubscriptions));
            this.collector.emit(values);
            this.sentSubscriptions++;
        } else {
            Utils.sleep(100);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("subscription"));
    }
    public static void main(String[] args) {
        SubscriptionSpout subscriptionSpout = new SubscriptionSpout(1000,new HashMap<String,Float>(){{
            put(STATION_ID, 0.92f);
            put(CITY, 0.1f);
            put(TEMPERATURE, 0.23f);
            put(RAIN, 0.49f);
            put(WIND, 0.12f);
            put(DIRECTION, 0.4426f);
            put(DATE, 0.4f);
        }});

        System.out.println(subscriptionSpout.generateSubscriptionEntries());
        System.out.println(subscriptionSpout.subscriptions.size());
        Map<String, Integer> freq = new HashMap<>();
        for(Subscription subscription : subscriptionSpout.subscriptions) {
            setFreqToZero(freq);
            for(SubscriptionEntry entry : subscription.getEntries())
                freq.put(entry.getField(),freq.get(entry.getField()) + 1);
            for(Map.Entry<String,Integer> frequency : freq.entrySet())
                if(frequency.getValue() > 1) {
                    System.out.println("AAAAAAAAAAA");
                    return;
                }

        }
        setFreqToZero(freq);
        for(Subscription subscription : subscriptionSpout.subscriptions){
            for(SubscriptionEntry entry : subscription.getEntries())
                freq.put(entry.getField(),freq.get(entry.getField()) + 1);
        }
        System.out.println(freq);
    }
    private static void setFreqToZero(Map<String,Integer> freqMap){
        freqMap.put(STATION_ID,0);
        freqMap.put(CITY, 0);
        freqMap.put(TEMPERATURE, 0);
        freqMap.put(RAIN, 0);
        freqMap.put(WIND,0);
        freqMap.put(DIRECTION, 0);
        freqMap.put(DATE, 0);

    }
}

