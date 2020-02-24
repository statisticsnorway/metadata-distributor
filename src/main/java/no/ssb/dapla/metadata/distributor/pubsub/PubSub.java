package no.ssb.dapla.metadata.distributor.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;

public interface PubSub {
    TopicAdminClient getTopicAdminClient();

    SubscriptionAdminClient getSubscriptionAdminClient();

    Publisher getPublisher(ProjectTopicName projectTopicName);

    Subscriber getSubscriber(ProjectSubscriptionName projectSubscriptionName, MessageReceiver messageReceiver);

    default boolean topicExists(TopicAdminClient topicAdminClient, ProjectName projectName, ProjectTopicName projectTopicName) {
        final int PAGE_SIZE = 25;
        TopicAdminClient.ListTopicsPagedResponse listResponse = topicAdminClient.listTopics(ListTopicsRequest.newBuilder().setProject(projectName.toString()).setPageSize(PAGE_SIZE).build());
        for (Topic topic : listResponse.iterateAll()) {
            if (topic.getName().equals(projectTopicName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = topicAdminClient.listTopics(ListTopicsRequest.newBuilder().setProject(projectName.toString()).setPageToken(listResponse.getNextPageToken()).setPageSize(PAGE_SIZE).build());
            for (Topic topic : listResponse.iterateAll()) {
                if (topic.getName().equals(projectTopicName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    default boolean subscriptionExists(SubscriptionAdminClient subscriptionAdminClient, ProjectName projectName, ProjectSubscriptionName projectSubscriptionName) {
        final int PAGE_SIZE = 25;
        SubscriptionAdminClient.ListSubscriptionsPagedResponse listResponse = subscriptionAdminClient.listSubscriptions(ListSubscriptionsRequest.newBuilder().setProject(projectName.toString()).setPageSize(PAGE_SIZE).build());
        for (Subscription subscription : listResponse.iterateAll()) {
            if (subscription.getName().equals(projectSubscriptionName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = subscriptionAdminClient.listSubscriptions(ListSubscriptionsRequest.newBuilder().setProject(projectName.toString()).setPageToken(listResponse.getNextPageToken()).setPageSize(PAGE_SIZE).build());
            for (Subscription subscription : listResponse.iterateAll()) {
                if (subscription.getName().equals(projectSubscriptionName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }
}