package com.jpmc.midascore.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.service.TransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final TransactionService transactionService;
    private final ObjectMapper objectMapper;

    public TransactionListener(TransactionService transactionService) {
        this.transactionService = transactionService;
        this.objectMapper = new ObjectMapper();
    }

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-group-task2"
    )
    public void listen(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            logger.info("Received transaction: {}", transaction);
            transactionService.processTransaction(transaction);
        } catch (Exception e) {
            logger.error("Error processing transaction message: {}", message, e);
        }
    }
}