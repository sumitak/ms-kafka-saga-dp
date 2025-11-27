package com.appsdeveloperblog.orders.saga;

import com.appsdeveloperblog.core.dto.commands.ReserveProductCommand;
import com.appsdeveloperblog.core.dto.events.OrderCreatedEvent;
import com.appsdeveloperblog.core.types.OrderStatus;
import com.appsdeveloperblog.orders.service.OrderHistoryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics="${orders.event.topic.name}")
public class OrderSaga {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productCommandTopicName;
    private final OrderHistoryService orderHistoryService;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.command.topic.name}") String productCommandTopicName,
                     OrderHistoryService orderHistoryService) {
        this.kafkaTemplate = kafkaTemplate;
        this.productCommandTopicName = productCommandTopicName;
        this.orderHistoryService = orderHistoryService;
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event) {
        // Handle create order command
        ReserveProductCommand command = new ReserveProductCommand(
            event.getProductId(),
            event.getProductQuantity(),
            event.getOrderId()
        );

        kafkaTemplate.send(productCommandTopicName, command);
        orderHistoryService.add(event.getOrderId(), OrderStatus.CREATED);
    }
}
