package Public;

import java.util.Map;

import DB.COVID19Lines;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomSerializer implements Serializer<COVID19Lines> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
    @Override
    public byte[] serialize(String topic, COVID19Lines data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
        retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception exception) {
        System.out.println("Error in serializing object"+ data);
        }
        return retVal;
    }
    @Override
    public void close() {
    }
}