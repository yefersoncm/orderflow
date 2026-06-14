package com.orderflow.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * order-service: dueno del agregado Order y del Saga Orchestrator.
 * F0: arranque base (web + actuator). La orquestacion de la saga llega en F2.
 */
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
