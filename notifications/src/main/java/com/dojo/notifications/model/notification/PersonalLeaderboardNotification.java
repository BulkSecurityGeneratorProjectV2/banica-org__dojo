package com.dojo.notifications.model.notification;

import com.dojo.notifications.model.client.CustomSlackClient;
import com.dojo.notifications.model.user.User;
import com.dojo.notifications.model.user.UserDetails;
import com.dojo.notifications.service.UserDetailsService;
import com.dojo.notifications.service.emailNotifier.MailContentBuilder;
import com.dojo.notifications.service.slackNotifier.SlackMessageBuilder;
import com.hubspot.slack.client.methods.params.chat.ChatPostMessageParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PersonalLeaderboardNotification extends LeaderboardNotification {

    private final UserDetails userDetails;

    public PersonalLeaderboardNotification(List<User> leaderboard, UserDetailsService userDetailsService, UserDetails userDetails) {
        super(leaderboard, userDetailsService);
        this.userDetails = userDetails;
    }

    @Override
    public ChatPostMessageParams getAsSlackNotification(SlackMessageBuilder slackMessageBuilder, CustomSlackClient slackClient, String slackChannel) {
        return slackMessageBuilder.generateSlackContent(userDetails, leaderboard, slackClient, slackChannel);
    }

    @Override
    public String getAsEmailNotification(MailContentBuilder mailContentBuilder) {
        Map<String, Object> contextParams = new HashMap<>();
        contextParams.put("leaderboard", leaderboard);
        contextParams.put("userDetails", userDetails);
        return mailContentBuilder.generateMailContent(contextParams);
    }
}
