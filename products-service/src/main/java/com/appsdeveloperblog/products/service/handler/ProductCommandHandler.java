package com.appsdeveloperblog.products.service.handler;

import com.appsdeveloperblog.core.dto.Product;
import com.appsdeveloperblog.core.dto.commands.CancelProductReservationCommand;
import com.appsdeveloperblog.core.dto.commands.ProcessPaymentCommand;
import com.appsdeveloperblog.core.dto.commands.ReserveProductCommand;
import com.appsdeveloperblog.core.dto.events.ProductReservationCancelledEvent;
import com.appsdeveloperblog.core.dto.events.ProductReservationFailedEvent;
import com.appsdeveloperblog.core.dto.events.ProductReservedEvent;
import com.appsdeveloperblog.products.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "${products.command.topic.name}")
public class ProductCommandHandler {

    private static Logger logger = LoggerFactory.getLogger(ProductCommandHandler.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productEventsTopicName;

    private final ProductService productService;

    public ProductCommandHandler(ProductService productService,
                                 KafkaTemplate<String, Object> kafkaTemplate,
                                 @Value("${products.events.topic.name}") String productEventsTopicName) {
        this.productService = productService;
        this.kafkaTemplate = kafkaTemplate;
        this.productEventsTopicName = productEventsTopicName;
    }

    @KafkaHandler
    public void handleCommand(@Payload ReserveProductCommand command) {
//if there is no enough stock then reserve method throws ProductInsufficientQuantityException
        try {
            Product desiredProduct = new Product(command.getProductId(), command.getProductQuantity());
            Product reservedProduct =productService.reserve(desiredProduct, command.getOrderId());
            ProductReservedEvent productReservedEvent = new ProductReservedEvent(command.getOrderId(),
                   command.getProductId(),
                    reservedProduct.getPrice(),
                    command.getProductQuantity());
            logger.info("**** Product reserved event created for order id: " + command.getOrderId());
            kafkaTemplate.send(productEventsTopicName, productReservedEvent);
        }catch(Exception ex){
            logger.error("**** Exception occurred while reserving product for order id: " + command.getOrderId(), ex);
            logger.error(ex.getLocalizedMessage(), ex);
          // here product reservation cancellation event can be sent to notify order service about failure
            //such that the order cannot be reserved.
          //  throw ex;
            ProductReservationFailedEvent productReservationFailedEvent = new ProductReservationFailedEvent(
                    command.getProductId(),
                    command.getOrderId(),
                    command.getProductQuantity());
            kafkaTemplate.send(productEventsTopicName, productReservationFailedEvent);
        }

    }

    @KafkaHandler
    public void handleCommand(CancelProductReservationCommand command) {
        logger.info("**** Received CancelProductReservationCommand for order id: " + command.getOrderId());
        Product productToCancel = new Product(command.getProductId(), command.getProductQuantity());
        productService.cancelReservation(productToCancel, command.getProductId());
        logger.info("**** Completed CancelProductReservationCommand for order id: " + command.getOrderId());

        ProductReservationCancelledEvent  productReservationCancelledEvent= new ProductReservationCancelledEvent(
                command.getProductId(),
                command.getOrderId());

        kafkaTemplate.send(productEventsTopicName, productReservationCancelledEvent);
    }
}
