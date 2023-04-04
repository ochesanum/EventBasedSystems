package tema1.pubsub;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public class SubscriptionEntry<T> {
    private String field;
    private String operator;
    private T value;
}
