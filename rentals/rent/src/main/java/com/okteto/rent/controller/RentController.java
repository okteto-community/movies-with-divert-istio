package com.okteto.rent.controller;

import com.okteto.rent.model.Rental;
import com.okteto.rent.repository.RentalRepository;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;

@RestController
public class RentController {
    private static final String KAFKA_TOPIC_RENTALS = "rentals";
    private static final String KAFKA_TOPIC_RETURNS = "returns";

    private final Logger logger = LoggerFactory.getLogger(RentController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private RentalRepository rentalRepository;

    @GetMapping(path= "/rent/healthz", produces = "application/json")
    Map<String, String> healthz( @RequestHeader(value = "baggage", required = false) String baggage) {
        String namespace = System.getenv("KUBERNETES_NAMESPACE");
        Map<String, String> response = new HashMap<>();
        response.put("status", "ok");
        response.put("namespace", namespace != null ? namespace : "");
        response.put("baggage", baggage != null ? baggage : "");

        return response;
    }

    @GetMapping(path= "/rent", produces = "application/json")
    List<Rental> getAllRentals( @RequestHeader(value = "baggage", required = false) String baggage) {
        logger.info("Fetching all rentals from database");
        logger.info("{} header received: {}", "baggage", baggage);
        List<Rental> allRentals = rentalRepository.findAll();

        // uncomment to increase price
        //for (Rental allRental : allRentals) {
        //    allRental.setPrice("68.99");
        //} 
        return allRentals;
    }
    
    @PostMapping(path= "/rent", consumes = "application/json", produces = "application/json")
    List<String> rent(@RequestBody Rental rentInput,
                      @RequestHeader(value = "baggage", required = false) String baggage) {
        String movieID = rentInput.getId();
        String price = rentInput.getPrice();

        logger.info("Rent [{},{}] received", movieID, price);

        // Create ProducerRecord to add custom headers
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC_RENTALS, movieID, price.toString());

        // Add baggage header to Kafka message if present
        if (baggage != null && !baggage.isEmpty()) {
            logger.info("Baggage header received: {}", baggage);
            record.headers().add(new RecordHeader("baggage", baggage.getBytes(StandardCharsets.UTF_8)));
        }

        kafkaTemplate.send(record)
        .thenAccept(result -> logger.info("Message [{}] delivered with offset {}",
                        movieID,
                        result.getRecordMetadata().offset()))
        .exceptionally(ex -> {
            logger.warn("Unable to deliver message [{}]. {}", movieID, ex.getMessage());
            return null;
        });


        return new LinkedList<>();
    }

    @PostMapping(path= "/rent/return", consumes = "application/json", produces = "application/json")
    public Map<String, String> returnMovie(@RequestBody ReturnRequest returnRequest,
                                           @RequestHeader(value = "baggage", required = false) String baggage) {
        String movieID = returnRequest.getMovieID();

        logger.info("Return [{}] received", movieID);

        // Create ProducerRecord to add custom headers
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC_RETURNS, movieID, movieID);

        // Add baggage header to Kafka message if present
        if (baggage != null && !baggage.isEmpty()) {
            logger.info("Baggage header received: {}", baggage);
            record.headers().add(new RecordHeader("baggage", baggage.getBytes(StandardCharsets.UTF_8)));
        }

        kafkaTemplate.send(record)
        .thenAccept(result -> logger.info("Return message [{}] delivered with offset {}",
                        movieID,
                        result.getRecordMetadata().offset()))
        .exceptionally(ex -> {
            logger.warn("Unable to deliver return message [{}]. {}", movieID, ex.getMessage());
            return null;
        });

        return Collections.singletonMap("status", "return processed");
    }

    public static class ReturnRequest {
        @JsonProperty("id")
        private String movieID;

        public void setMovieID(String movieID) {
            this.movieID = movieID;
        }

        public String getMovieID() {
            return movieID;
        }
    }
}
