package kafka.project1;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new TwitterProducer().run(args[0], args[1]);
    }

    public TwitterProducer() {

    }

    public void run(String username, String password) throws InterruptedException {
        final Logger logger = LoggerFactory.getLogger(TwitterProducerSolution.class);

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(232415629L);
        List<String> terms = Lists.newArrayList("dunkin");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new BasicAuth(
                "consumerKey",
                "consumerSecret");

        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(hosebirdEndpoint)
                .authentication(hosebirdAuth)
                .processor(new StringDelimitedProcessor(msgQueue))
                .build();

        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Establish a connection
        client.connect();

        for (int msgRead = 0; msgRead < 4; msgRead++) {
            if (client.isDone()) {
                System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
                break;
            }

            String msg = msgQueue.take();

            System.out.println(msg);

            String topic = "twitter";
            String key = "id_" + Integer.toString(msgRead);

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, msg);

            logger.info("Key: " + key); // log the key

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

        client.stop();

        System.out.printf("The client read %d messages! \n", client.getStatsTracker().getNumMessages());

    }
}

