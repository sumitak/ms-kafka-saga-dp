package com.appsdeveloperblog.payments.service.handler;

import com.appsdeveloperblog.core.dto.Payment;
import com.appsdeveloperblog.core.dto.commands.ProcessPaymentCommand;
import com.appsdeveloperblog.core.dto.events.PaymentProcessedEvent;
import com.appsdeveloperblog.core.dto.events.PaymentsFailedEvent;
import com.appsdeveloperblog.core.exceptions.CreditCardProcessorUnavailableException;
import com.appsdeveloperblog.payments.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics="${payments.commands.topic.name}")
public class PaymentsCommandsHandler {
    private static Logger logger = LoggerFactory.getLogger(PaymentsCommandsHandler.class);
    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> paymentKafkaTemplate;
    private final String paymentsEventsTopicName;

    public PaymentsCommandsHandler(PaymentService paymentService,
                                   KafkaTemplate<String, Object> paymentKafkaTemplate,
                                   @Value("${payments.events.topic.name}") String paymentsEventsTopicName) {
        this.paymentService = paymentService;
        this.paymentKafkaTemplate = paymentKafkaTemplate;
        this.paymentsEventsTopicName = paymentsEventsTopicName;
    }

    @KafkaHandler
    public void handleCommand(@Payload ProcessPaymentCommand command) {

        try {
            Payment payment = new Payment(command.getOrderId(),
                    command.getProductId(),
                    command.getProductPrice(),
                    command.getProductQuantity());
            Payment processedPayment = paymentService.process(payment);
            PaymentProcessedEvent paymentProcessedEvent = new PaymentProcessedEvent(
                    processedPayment.getOrderId(),
                    processedPayment.getId()
            );
            logger.info("**** Payment processed event created for order id: " + command.getOrderId());
            paymentKafkaTemplate.send(paymentsEventsTopicName, paymentProcessedEvent);
            logger.info("**** Payment processed event sent to topic for order id: " + command.getOrderId());

        } catch (CreditCardProcessorUnavailableException ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            PaymentsFailedEvent paymentsFailedEvent = new PaymentsFailedEvent(command.getOrderId(),
                    command.getProductId(),
                    command.getProductQuantity());
            paymentKafkaTemplate.send(paymentsEventsTopicName, paymentsFailedEvent);
        }


    }
}
