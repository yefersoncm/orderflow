package com.orderflow.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * inventory-service: reserva y libera stock de forma idempotente.
 * F0: arranque base (web + actuator). La logica de reserva/compensacion llega en F2/F3.
 */
@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
