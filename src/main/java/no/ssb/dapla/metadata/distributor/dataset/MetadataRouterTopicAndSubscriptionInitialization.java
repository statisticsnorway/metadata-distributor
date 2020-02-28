package no.ssb.dapla.metadata.distributor.dataset;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import io.helidon.config.Config;
import no.ssb.pubsub.PubSub;
import no.ssb.pubsub.PubSubAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MetadataRouterTopicAndSubscriptionInitialization {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRouterTopicAndSubscriptionInitialization.class);

    public static void initializeTopicsAndSubscriptions(Config routeConfig, PubSub pubSub) {
        List<Config> upstreams = routeConfig.get("upstream").asNodeList().get();
        List<Config> downstreams = routeConfig.get("downstream").asNodeList().get();

        try (TopicAdminClient topicAdminClient = pubSub.getTopicAdminClient()) {
            try (SubscriptionAdminClient subscriptionAdminClient = pubSub.getSubscriptionAdminClient()) {

                for (Config downstream : downstreams) {
                    String downstreamProjectId = downstream.get("projectId").asString().get();
                    String downstreamTopic = downstream.get("topic").asString().get();

                    LOG.info("Initializing downstream topic: {}", downstreamTopic);
                    PubSubAdmin.createTopicIfNotExists(topicAdminClient, downstreamProjectId, downstreamTopic);
                }

                for (Config upstream : upstreams) {
                    String upstreamProjectId = upstream.get("projectId").asString().get();
                    String upstreamTopicName = upstream.get("topic").asString().get();
                    String upstreamSubscriptionName = upstream.get("subscription").asString().get();
                    int upstreamAckDeadlineSeconds = upstream.get("ack-deadline-seconds").asInt().orElse(30);
                    Config upstreamDlq = upstream.get("dlq");
                    String upstreamDlqProjectId = upstreamDlq.get("projectId").asString().orElse(null);
                    String upstreamDlqTopic = upstreamDlq.get("topic").asString().orElse(null);
                    int upstreamDlqMaxRedeliveryAttempts = upstreamDlq.get("max-redelivery-attempts").asInt().orElse(10);
                    String upstreamDlqSubscription = upstreamDlq.get("subscription").asString().orElse(null);

                    LOG.info("Initializing upstream topic: {}", upstreamTopicName);
                    PubSubAdmin.createTopicIfNotExists(topicAdminClient, upstreamProjectId, upstreamTopicName);

                    if (upstreamDlqTopic != null) {

                        LOG.info("Initializing DLQ: {}", upstreamDlqTopic);
                        PubSubAdmin.createTopicIfNotExists(topicAdminClient, upstreamDlqProjectId, upstreamDlqTopic);

                        if (upstreamDlqSubscription != null) {
                            PubSubAdmin.createSubscriptionIfNotExists(subscriptionAdminClient, upstreamDlqProjectId,
                                    upstreamDlqTopic, upstreamDlqSubscription, 60);
                        }
                    }

                    PubSubAdmin.createSubscriptionIfNotExists(subscriptionAdminClient, upstreamProjectId,
                            upstreamTopicName, upstreamSubscriptionName, upstreamAckDeadlineSeconds,
                            upstreamDlqMaxRedeliveryAttempts, upstreamDlqProjectId, upstreamDlqTopic);
                }
            }
        }
    }
}
