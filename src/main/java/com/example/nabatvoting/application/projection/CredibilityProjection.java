package com.example.nabatvoting.application.projection;

import com.example.nabatvoting.domain.event.VoteCastEvent;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class CredibilityProjection {

    private final Map<String, Integer> scoresByAlertId = new ConcurrentHashMap<>();

    public void apply(VoteCastEvent event) {
        int delta = switch (event.voteType()) {
            case UPVOTE -> 1;
            case DOWNVOTE -> -1;
            case CONFIRM -> 2;
        };
        scoresByAlertId.merge(event.alertId(), delta, Integer::sum);
    }

    public int getScore(String alertId) {
        return scoresByAlertId.getOrDefault(alertId, 0);
    }
}
