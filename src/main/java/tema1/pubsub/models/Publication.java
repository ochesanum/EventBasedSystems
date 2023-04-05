package tema1.pubsub.models;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class Publication implements Serializable {

    private String stationId;
    private String city;
    private int temp;
    private double rain;
    private int wind;
    private String direction;
    private String date;
}
