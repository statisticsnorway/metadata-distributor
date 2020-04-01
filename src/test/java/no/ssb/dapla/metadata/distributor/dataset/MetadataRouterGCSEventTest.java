package no.ssb.dapla.metadata.distributor.dataset;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.common.util.concurrent.MoreExecutors;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.metadata.distributor.MetadataDistributorApplication;
import no.ssb.helidon.media.protobuf.ProtobufJsonUtils;
import no.ssb.pubsub.PubSub;
import no.ssb.testing.helidon.ConfigOverride;
import no.ssb.testing.helidon.IntegrationTestExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@ConfigOverride({
        "pubsub.use-emulator", "false",
        "pubsub.credential-provider", "service-account",
        "pubsub.credentials.service-account.path", "secret/dev-sirius-37ed83f3c726.json",
        "pubsub.admin", "false",
        "pubsub.metadata-routing.0.upstream.0.projectId", "dev-sirius",
        "pubsub.metadata-routing.0.upstream.0.topic", "md-test-up",
        "pubsub.metadata-routing.0.upstream.0.subscription", "md-test-up-gcs",
        "pubsub.metadata-routing.0.upstream.0.subscribe", "false",
        "pubsub.metadata-routing.0.upstream.0.ack-deadline-seconds", "30",
        "pubsub.metadata-routing.0.upstream.0.dlq.projectId", "dev-sirius",
        "pubsub.metadata-routing.0.upstream.0.dlq.topic", "md-test-up-dlq",
        "pubsub.metadata-routing.0.upstream.0.dlq.max-redelivery-attempts", "5",
        "pubsub.metadata-routing.0.upstream.0.dlq.subscription", "md-test-up-dlq-errors",
        "pubsub.metadata-routing.0.downstream.0.projectId", "dev-sirius",
        "pubsub.metadata-routing.0.downstream.0.topic", "md-test-down",
        "storage.cloud-storage.enabled", "true",
        "storage.cloud-storage.credential-provider", "service-account",
        "storage.cloud-storage.credentials.service-account.path", "secret/dev-sirius-37ed83f3c726.json",
})
@ExtendWith(IntegrationTestExtension.class)
public class MetadataRouterGCSEventTest {

    @Inject
    MetadataDistributorApplication application;

    @Test
    public void thatGCSEventCanBeParsed() throws InterruptedException {
        Storage storage = application.get(Storage.class);
        PubSub pubSub = application.get(PubSub.class);
        MetadataSignatureVerifier metadataSignatureVerifier = (data, receivedSign) -> true;

        String bucketName = "ssb-dev-md-test";

        if (storage.get(bucketName) == null) {
            Bucket bucket = storage.create(BucketInfo.newBuilder(bucketName)
                    .setStorageClass(StorageClass.STANDARD)
                    .setLocation("EUROPE-NORTH1")
                    .build());
            System.out.printf("Bucket %s created.%n", bucket.getName());
        }

        long version = System.currentTimeMillis();

        System.out.printf("Publishing two files under version folder: %d%n", version);

        String metadataPath = String.format("%s/%d/%s", "md-junit-test", version, ".dataset-meta.json");
        String metadataSignaturePath = String.format("%s/%d/%s", "md-junit-test", version, ".dataset-meta.json.sign");

        storage.create(BlobInfo.newBuilder(BucketInfo.of(bucketName), metadataPath).build(), ProtobufJsonUtils.toString(DatasetMeta.newBuilder().build()).getBytes(StandardCharsets.UTF_8));
        storage.create(BlobInfo.newBuilder(BucketInfo.of(bucketName), metadataSignaturePath).build(), "My Signature".getBytes(StandardCharsets.UTF_8));

        String upstreamProjectId = "dev-sirius";
        String upstreamTopicName = "md-test-up";
        String upstreamSubscriptionName = "md-test-up-gcs";

        CountDownLatch latch = new CountDownLatch(2);

        MessageReceiver messageReceiver = (message, consumer) -> {
            try {
                System.out.printf("message: %s%n", message);

                MetadataRouter.process(storage, metadataSignatureVerifier, Collections.emptyList(), upstreamTopicName, upstreamSubscriptionName, message, consumer::ack);

                Map<String, String> attributes = message.getAttributesMap();
                String objectId = attributes.get("objectId");
                if (metadataPath.equals(objectId)) {
                    latch.countDown();
                } else if (metadataSignaturePath.equals(objectId)) {
                    latch.countDown();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        };

        Subscriber subscriber = pubSub.getSubscriber(upstreamProjectId, upstreamSubscriptionName, messageReceiver);
        subscriber.addListener(
                new Subscriber.Listener() {
                    public void failed(Subscriber.State from, Throwable failure) {
                        System.out.printf("Error with subscriber on subscription: '%s'", upstreamSubscriptionName);
                    }
                },
                MoreExecutors.directExecutor());
        ApiService subscriberStartAsyncApiService = subscriber.startAsync();

        subscriberStartAsyncApiService.awaitRunning();

        latch.await(1, TimeUnit.MINUTES);
    }
}


