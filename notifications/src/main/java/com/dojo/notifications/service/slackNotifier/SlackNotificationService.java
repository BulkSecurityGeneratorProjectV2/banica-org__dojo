package com.dojo.notifications.service.slackNotifier;

import com.dojo.notifications.contest.Contest;
import com.dojo.notifications.contest.enums.NotifierType;
import com.dojo.notifications.model.client.CustomSlackClient;
import com.dojo.notifications.model.client.SlackClientManager;
import com.dojo.notifications.model.notification.Notification;
import com.dojo.notifications.model.user.UserDetails;
import com.dojo.notifications.service.NotificationService;
import com.hubspot.slack.client.methods.params.chat.ChatPostMessageParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SlackNotificationService implements NotificationService {

    private final SlackClientManager slackClientManager;
    private final SlackMessageBuilder slackMessageBuilder;

    @Autowired
    public SlackNotificationService(SlackClientManager slackClientManager, SlackMessageBuilder slackMessageBuilder) {
        this.slackClientManager = slackClientManager;
        this.slackMessageBuilder = slackMessageBuilder;
    }

    @Override
    public NotifierType getNotificationServiceTypeMapping() {
        return NotifierType.SLACK;
    }

    // Notify user
    @Override
    public void notify(UserDetails userDetails, Notification notification, Contest contest) {
        CustomSlackClient slackClient = getSlackClient(contest);
        String slackChannel = slackClient.getConversationId(userDetails.getEmail());
        slackClient.postMessage(convertToSlackNotification(notification, slackClient, slackChannel));
    }

    // Notify channel
    @Override
    public void notify(Notification notification, Contest contest) {
        CustomSlackClient slackClient = getSlackClient(contest);
        String slackChannel = contest.getSlackChannel();
        slackClient.postMessage(convertToSlackNotification(notification, slackClient, slackChannel));
    }

    private ChatPostMessageParams convertToSlackNotification(Notification notification, CustomSlackClient slackClient, String slackChannel) {
        return notification
                .convertToSlackNotification(slackMessageBuilder, slackClient)
                .setChannelId(slackChannel)
                .build();
    }

    private CustomSlackClient getSlackClient(Contest contest) {
        return slackClientManager.getSlackClient(contest.getSlackToken());
    }
}
