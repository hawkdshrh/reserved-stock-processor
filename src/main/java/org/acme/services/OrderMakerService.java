package org.acme.services;

import javax.enterprise.context.ApplicationScoped;

import org.acme.beans.Order;
import org.acme.beans.OrderEntry;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
public class OrderMakerService {

    @Inject
    @Channel("orders-out")
    Emitter<Order> emitter;

    private static final Logger LOGGER = Logger.getLogger("OrderMakerService");

    public void orderStock(Order order) {

        for (OrderEntry entry : order.getOrderEntries()) {
            LOGGER.log(Level.INFO, "Ordering {0} items for sku:{1}.", new Object[]{entry.getQuantity(), entry.getProduct().getProductSku()});
        }
        emitter.send(order);
    }

}
