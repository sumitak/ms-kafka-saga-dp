package com.appsdeveloperblog.orders.saga;

import com.appsdeveloperblog.core.dto.commands.ApprovedOrderCommand;
import com.appsdeveloperblog.core.dto.commands.ProcessPaymentCommand;
import com.appsdeveloperblog.core.dto.commands.ReserveProductCommand;
import com.appsdeveloperblog.core.dto.events.OrderApprovedEvent;
import com.appsdeveloperblog.core.dto.events.OrderCreatedEvent;
import com.appsdeveloperblog.core.dto.events.PaymentProcessedEvent;
import com.appsdeveloperblog.core.dto.events.ProductReservedEvent;
import com.appsdeveloperblog.core.types.OrderStatus;
import com.appsdeveloperblog.orders.service.OrderHistoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics={"${orders.event.topic.name}",
        "${products.events.topic.name}",
        "${payments.events.topic.name}"}
)
public class OrderSaga {
private final Logger logger = LoggerFactory.getLogger(OrderSaga.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productCommandTopicName;
    private final OrderHistoryService orderHistoryService;
    private final String paymentsCommandTopicName;
    private final String ordersCommandsTopicName;

    public OrderSaga(KafkaTemplate<String, Object> kafkaTemplate,
                     @Value("${products.command.topic.name}") String productCommandTopicName,
                     OrderHistoryService orderHistoryService,
                     @Value("${payments.commands.topic.name}") String paymentsCommandTopicName,
                     @Value("${orders.commands.topic.name}") String ordersCommandsTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productCommandTopicName = productCommandTopicName;
        this.orderHistoryService = orderHistoryService;
        this.paymentsCommandTopicName = paymentsCommandTopicName;
        this.ordersCommandsTopicName = ordersCommandsTopicName;
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event) {
        // Handle create order command
        logger.info("**** Received OrderCreatedEvent for order id: " + event.getOrderId());
        ReserveProductCommand command = new ReserveProductCommand(
            event.getProductId(),
            event.getProductQuantity(),
            event.getOrderId()
        );

        kafkaTemplate.send(productCommandTopicName, command);
        orderHistoryService.add(event.getOrderId(), OrderStatus.CREATED);
        logger.info("**** Sent ReserveProductCommand for order id: " + event.getOrderId());
    }

    /**
     * Saga is requesting for payment to be processed and since this command has to do with payments,
     * let's make payments microservice handle it and to process it.
     */


    @KafkaHandler
    public void handleEvent(@Payload ProductReservedEvent event) {
        // Handle product reserved event
        logger.info("**** Received ProductReservedEvent for order id: " + event.getOrderId());
        ProcessPaymentCommand command = new ProcessPaymentCommand(
            event.getOrderId(),
            event.getProductId(),
            event.getProductPrice(),
            event.getProductQuantity()
        );
        kafkaTemplate.send(paymentsCommandTopicName, command);
        logger.info("**** Sent ProcessPaymentCommand for order id: " + event.getOrderId());

    }

    /**
     * now the payment procesesed event is handled, the next step
     * is to notify shipment microservice to ship the order that the
     * payment is successful.
     *
     * But here, instructor is ending flow here for simplicity.
     * Publising an approved order command to order service.
     *
     * This command will be handled by orders microservice and it will
     * update the order status to approved.
     */


    @KafkaHandler
    public void handleEvent(@Payload PaymentProcessedEvent event) {
        //default handler
        logger.info("**** Received PaymentProcessedEvent for order id: " + event.getOrderId());
        ApprovedOrderCommand  approvedOrderCommand = new ApprovedOrderCommand(event.getOrderId());
        kafkaTemplate.send(ordersCommandsTopicName, approvedOrderCommand);
        logger.info("**** Sent ApprovedOrderCommand for order id: " + event.getOrderId());
    }

    /**
     * end of saga happy path, order is approved
     * @param event
     */

    @KafkaHandler
    public void handleEvent(@Payload OrderApprovedEvent event) {
        // simple save order status, since end of the flow
        logger.info("**** Received OrderApprovedEvent for order id: " + event.getOrderId());
        orderHistoryService.add(event.getOrderId(), OrderStatus.APPROVED);
        logger.info("**** Order saga completed for order id: " + event.getOrderId());
    }

}
