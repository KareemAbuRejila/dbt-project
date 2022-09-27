package Public;

public interface IKafkaConstants {
    String KAFKA_BROKERS = "localhost:9092";
    Integer MESSAGE_COUNT = 1000000;
    String CLIENT_ID = "client1";
    String GROUP_ID_CONFIG = "consumerGroup1";
    Integer MAX_NO_MESSAGE_FOUND_COUNT = 30;
    String OFFSET_RESET_LATEST = "latest";
    String OFFSET_RESET_EARLIER = "earliest";
    Integer MAX_POLL_RECORDS = 1;


    String TOPIC_NAME = "COVID19";
    String TOPIC_NAME2 = "COVID19_LINES";


}