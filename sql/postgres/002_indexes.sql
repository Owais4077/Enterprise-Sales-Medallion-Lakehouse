-- Indexes beyond the primary keys / unique constraints.
--   * Foreign-key columns: Postgres does not index FKs automatically; joins and
--     cascading checks need them.
--   * updated_at: the incremental-extraction watermark filter
--     (WHERE updated_at > :last_watermark) must not scan the whole table.

CREATE INDEX idx_customers_updated_at   ON sales.customers (updated_at);
CREATE INDEX idx_products_updated_at    ON sales.products (updated_at);

CREATE INDEX idx_orders_customer_id     ON sales.orders (customer_id);
CREATE INDEX idx_orders_order_date      ON sales.orders (order_date);
CREATE INDEX idx_orders_updated_at      ON sales.orders (updated_at);

CREATE INDEX idx_order_items_order_id   ON sales.order_items (order_id);
CREATE INDEX idx_order_items_product_id ON sales.order_items (product_id);
CREATE INDEX idx_order_items_updated_at ON sales.order_items (updated_at);

CREATE INDEX idx_payments_order_id      ON sales.payments (order_id);
CREATE INDEX idx_payments_updated_at    ON sales.payments (updated_at);
