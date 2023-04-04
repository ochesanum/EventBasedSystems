package tema1.pubsub;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class Publication implements Serializable {

    private int stationId;
    private String city;
    private int temp;
    private double rain;
    private int wind;
    private String direction;
    private String date;
}
