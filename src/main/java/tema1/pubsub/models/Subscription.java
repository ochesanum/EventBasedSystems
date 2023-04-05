package tema1.pubsub.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@AllArgsConstructor
@ToString
public class Subscription implements Serializable {
    private List<SubscriptionEntry> entries;

    public Subscription() {
        this.entries = new ArrayList<>();
    }
}
