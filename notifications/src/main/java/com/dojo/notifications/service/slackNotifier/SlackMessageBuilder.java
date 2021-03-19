package com.dojo.notifications.service.slackNotifier;

import com.dojo.notifications.model.client.CustomSlackClient;
import com.dojo.notifications.model.leaderboard.Leaderboard;
import com.dojo.notifications.model.user.UserDetails;
import com.dojo.notifications.service.UserDetailsService;
import com.hubspot.slack.client.methods.params.chat.ChatPostMessageParams;
import org.springframework.stereotype.Service;

@Service
public abstract class SlackMessageBuilder {

    public abstract ChatPostMessageParams generateSlackContent(UserDetails userDetails, Leaderboard leaderboard, UserDetailsService userDetailsService, CustomSlackClient slackClient, String slackChannel);
    public abstract ChatPostMessageParams generateSlackContent(Leaderboard leaderboard, UserDetailsService userDetailsService, CustomSlackClient slackClient, String slackChannel);
}
