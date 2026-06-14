package com.orderflow.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * notification-service: consume eventos terminales (OrderConfirmed/OrderCancelled) y notifica.
 * F0: arranque base (web + actuator). El consumo de eventos llega en F3.
 */
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
