package com.appsdeveloperblog.orders.service.handler;

import com.appsdeveloperblog.core.dto.commands.ApprovedOrderCommand;
import com.appsdeveloperblog.orders.saga.OrderSaga;
import com.appsdeveloperblog.orders.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics="${orders.commands.topic.name}")
public class OrderCommandsHandler {
    private final Logger logger = LoggerFactory.getLogger(OrderCommandsHandler.class);
    private final OrderService orderService;

    public OrderCommandsHandler(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaHandler
    public void handleCommand(@Payload ApprovedOrderCommand approvedOrderCommand) {
        // No commands to handle for now
        logger.info("Received approved order command {}", approvedOrderCommand);
        orderService.approveOrder(approvedOrderCommand.getOrderId());;
        logger.info("Approved order command {}", approvedOrderCommand);
    }

}
