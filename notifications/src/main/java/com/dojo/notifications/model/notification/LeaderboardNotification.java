package com.dojo.notifications.model.notification;

import com.dojo.notifications.model.leaderboard.Leaderboard;
import com.dojo.notifications.service.UserDetailsService;

import java.util.HashMap;
import java.util.Map;

public abstract class LeaderboardNotification implements Notification {

    protected static final String LEADERBOARD_KEY = "leaderboard";
    protected static final String USERDETAILS_KEY = "userDetails";

    protected final UserDetailsService userDetailsService;
    protected final Leaderboard leaderboard;

    public LeaderboardNotification(UserDetailsService userDetailsService, Leaderboard leaderboard) {
        this.userDetailsService = userDetailsService;
        this.leaderboard = leaderboard;
    }

    protected Map<String, Object> getContextParams() {
        Map<String, Object> contextParams = new HashMap<>();
        contextParams.put(LEADERBOARD_KEY, leaderboard.getParticipants());
        return contextParams;
    }
}
