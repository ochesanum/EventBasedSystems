package tema1.pubsub;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@AllArgsConstructor
@ToString
public class Subscription {
    private List<SubscriptionEntry> entries;

    public Subscription() {
        this.entries = new ArrayList<>();
    }
}
