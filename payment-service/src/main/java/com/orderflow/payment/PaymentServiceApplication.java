package com.orderflow.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * payment-service: procesa pagos contra un gateway simulado, de forma idempotente.
 * F0: arranque base (web + actuator). La logica de cobro/compensacion llega en F2/F3.
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
