package Public;

import java.util.Map;

import DB.COVID19Lines;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CustomObjectDeserializer implements Deserializer<COVID19Lines> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	@Override
	public COVID19Lines deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		COVID19Lines object = null;
		try {
			object = mapper.readValue(data, COVID19Lines.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	@Override
	public void close() {
	}
}