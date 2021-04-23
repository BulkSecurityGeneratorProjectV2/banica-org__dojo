package com.dojo.notifications.service.messageGenerator.mail;

import com.dojo.notifications.model.notification.enums.NotificationType;
import org.springframework.stereotype.Service;
import org.thymeleaf.ITemplateEngine;
import org.thymeleaf.context.Context;

import java.util.Map;

@Service
public class LeaderboardMailMessageGenerator extends MailMessageGenerator {

    public LeaderboardMailMessageGenerator(ITemplateEngine templateEngine) {
        super(templateEngine);
    }

    @Override
    public NotificationType getMessageGeneratorTypeMapping() {
        return NotificationType.LEADERBOARD;
    }

    @Override
    public String generateMessage(Map<String, Object> contextParams) {
        Context context = new Context();
        contextParams.forEach(context::setVariable);

        return getTemplateEngine().process("leaderboardMailTemplate", context);
    }

}
