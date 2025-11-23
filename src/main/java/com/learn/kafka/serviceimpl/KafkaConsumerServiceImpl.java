package com.learn.kafka.serviceimpl;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.learn.kafka.dto.UserDto;

@Service
public class KafkaConsumerServiceImpl {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerServiceImpl.class);

	@KafkaListener(topics = "string-topic", groupId = "kafka-group")
	public void consumeString(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload String value) {
		try {
			log.info("Consumed String - Key: {}, Value: {}", key, value);
		} catch (Exception e) {
			log.error("Error consuming String: {}", e.getMessage());
		}
	}

	@KafkaListener(topics = "dto-topic", groupId = "kafka-group")
	public void consumeDto(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload UserDto dto) {
		try {
			log.info("Consumed DTO - Key: {}, DTO: {}", key, dto);
		} catch (Exception e) {
			log.error("Error consuming DTO: {}", e.getMessage());
		}
	}

	@KafkaListener(topics = "map-topic", groupId = "kafka-group")
	public void consumeMap(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload Map<String, Object> map) {
		try {
			log.info("Consumed Map - Key: {}, Map: {}", key, map);
		} catch (Exception e) {
			log.error("Processing failed for key {}: {}", key, e.getMessage());
		}
	}

	@KafkaListener(topics = "list-topic", groupId = "kafka-group")
	public void consumeList(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload List<String> list) {
		try {
			log.info("Consumed List - Key: {}, List: {}", key, list);
		} catch (Exception e) {
			log.error("Processing failed for key {}: {}", key, e.getMessage());
		}
	}

}
