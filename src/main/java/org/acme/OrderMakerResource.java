package org.acme;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.acme.beans.Product;
import org.acme.beans.Order;
import org.acme.beans.OrderEntry;
import org.acme.services.OrderMakerService;

@Path("/orders")
public class OrderMakerResource {

    private static final Logger LOGGER = Logger.getLogger("OrderMakerResource");

    @Inject
    OrderMakerService orderMakerService;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{sku}/{orderId}/{orderCode}")
    public Boolean orderQuantity(@PathParam("sku") String sku, @PathParam("orderId") Long orderId, @PathParam("orderCode") String orderCode, OrderEntry entry) {

        /**
         * Example Payload:
         * {"product":{"productName":"bananas","productSku":"1111-2222-3333-1115"},"quantity":254}
         */
        LOGGER.log(Level.INFO, "Updating order:{0} with {1] for {2} items.", new Object[]{orderId, entry.getProduct().getProductSku(), entry.getQuantity()});
        try {
            Order order = new Order();
            order.setOrderCode(orderCode);
            order.setOrderEntries(new OrderEntry[]{entry});
            orderMakerService.orderStock(order);
        } catch (Throwable t) {
            System.err.println(t.getLocalizedMessage());
            return false;
        }
        return true;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("")
    public Boolean orderQuantities(Order order) {

        /**
         * Example Payload:
         * {"orderId":"103427","orderEntries":[{"product":{"productName":"peaches","productSku":"1111-2222-3333-1226"},"quantity":30},{"product":{"productName":"cherries","productSku":"1111-2222-3333-1122"},"quantity":30}],"orderCode":"5432-0000-1111-2160"}
         */
        if (order == null || order.getOrderEntries() == null || order.getOrderEntries().length <= 0) {
            LOGGER.log(Level.INFO, "Invalid order request.");
            return false;
        }

        LOGGER.log(Level.INFO, "Ordering {0} item types.", new Object[]{order.getOrderEntries().length});

        for (OrderEntry entry : order.getOrderEntries()) {
            LOGGER.log(Level.INFO, "Ordering: {0} items for sku: {1}.", new Object[]{entry.getQuantity(), entry.getProduct().getProductSku()});
        }
        
        try {
        orderMakerService.orderStock(order);
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, t.getLocalizedMessage());
            return false;
        }

        return true;
    }

}
