package org.example.task2.spring.modulith.shop;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A small Spring Modulith application with three modules: orders pushes and publishes
 * OrderPlaced; shipping turns that event into a shipment and invoices it; billing runs the
 * invoices. Each module owns its topics and exposes them as TopicRefs in a named interface.
 */
@SpringBootApplication(excludeName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
public class ShopApplication {
}
