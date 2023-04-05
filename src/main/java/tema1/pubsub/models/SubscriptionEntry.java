package tema1.pubsub.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@AllArgsConstructor
@Getter
@ToString
@NoArgsConstructor
public class SubscriptionEntry<T> implements Serializable {
    private String field;
    private String operator;
    private T value;
}
