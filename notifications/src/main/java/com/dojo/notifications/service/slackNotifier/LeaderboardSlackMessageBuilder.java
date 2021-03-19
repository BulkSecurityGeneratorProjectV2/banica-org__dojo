package com.dojo.notifications.service.slackNotifier;

import com.dojo.notifications.model.client.CustomSlackClient;
import com.dojo.notifications.model.leaderboard.Leaderboard;
import com.dojo.notifications.model.notification.SlackNotificationUtils;
import com.dojo.notifications.model.user.UserDetails;
import com.dojo.notifications.service.LeaderboardService;
import com.hubspot.slack.client.methods.params.chat.ChatPostMessageParams;
import com.hubspot.slack.client.models.Attachment;
import com.hubspot.slack.client.models.actions.Action;
import com.hubspot.slack.client.models.actions.ActionType;
import com.hubspot.slack.client.models.blocks.Divider;
import com.hubspot.slack.client.models.blocks.Section;
import com.hubspot.slack.client.models.blocks.objects.Text;
import com.hubspot.slack.client.models.blocks.objects.TextType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LeaderboardSlackMessageBuilder extends SlackMessageBuilder {

    private static final String PERSONAL_TITLE = "Your position in leaderboard has changed";
    private static final String COMMON_TITLE = "Leaderboard update";

    private static final String BUTTON_TEXT = "View Leaderboard in Dojorena";
    private static final String BUTTON_STYLE = "primary";
    //TODO Change this to real url
    private static final String BUTTON_REDIRECT_URL = "http://localhost:8081/api/v1/codenjoy/leaderboard";
    private static final String USER = "*User*";
    private static final String SCORE = "*Score*";

    private static final UserDetails COMMON = null;

    private LeaderboardService leaderboardService;

    @Autowired
    public LeaderboardSlackMessageBuilder(LeaderboardService leaderboardService) {
        this.leaderboardService = leaderboardService;
    }

    @Override
    public ChatPostMessageParams generateSlackContent(UserDetails userDetails, Leaderboard leaderboard, CustomSlackClient slackClient, String slackChannel) {
        Text leaderboardNames = leaderboardService.buildLeaderboardNames(leaderboard, userDetails, slackClient);
        Text leaderboardScores = leaderboardService.buildLeaderboardScores(leaderboard, userDetails);

        return getChatPostMessageParams(slackChannel, leaderboardNames, leaderboardScores, PERSONAL_TITLE);
    }

    @Override
    public ChatPostMessageParams generateSlackContent(Leaderboard leaderboard, CustomSlackClient slackClient, String slackChannel) {
        Text leaderboardNames = leaderboardService.buildLeaderboardNames(leaderboard, COMMON, slackClient);
        Text leaderboardScores = leaderboardService.buildLeaderboardScores(leaderboard, COMMON);

        return getChatPostMessageParams(slackChannel, leaderboardNames, leaderboardScores, COMMON_TITLE);
    }

    private ChatPostMessageParams getChatPostMessageParams(String slackChannel, Text leaderboardNames, Text leaderboardScores, String personalTitle) {
        ChatPostMessageParams.Builder builder = ChatPostMessageParams.builder();
        addDivider(builder);
        addUsersWithScores(builder, personalTitle, leaderboardNames, leaderboardScores);
        addDivider(builder);
        addRedirectButton(builder);
        builder.setChannelId(slackChannel);
        return builder.build();
    }

    private void addDivider(ChatPostMessageParams.Builder builder) {
        builder.addBlocks(Divider.builder().build());
    }

    private void addUsersWithScores(ChatPostMessageParams.Builder builder, String title, Text leaderboardNames, Text leaderboardScores) {
        builder.addBlocks(
                Section.of(Text.of(TextType.MARKDOWN, SlackNotificationUtils.makeBold(title)))
                        .withFields(
                                Text.of(TextType.MARKDOWN, USER),
                                Text.of(TextType.MARKDOWN, SCORE),
                                leaderboardNames,
                                leaderboardScores));
    }

    private void addRedirectButton(ChatPostMessageParams.Builder builder) {
        builder.addAttachments(Attachment.builder()
                .addActions(Action.builder()
                        .setType(ActionType.BUTTON)
                        .setText(BUTTON_TEXT)
                        .setRawStyle(BUTTON_STYLE)
                        .setUrl(BUTTON_REDIRECT_URL)
                        .build())
                .build());
    }
}
