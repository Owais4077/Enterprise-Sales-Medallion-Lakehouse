-- Keep updated_at honest: any UPDATE bumps it automatically, so the incremental
-- pipeline can never miss a change because an application forgot to set it.
-- INSERTs keep whatever timestamp is supplied (needed to load historical data).

CREATE OR REPLACE FUNCTION sales.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated_at   BEFORE UPDATE ON sales.customers
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_products_updated_at    BEFORE UPDATE ON sales.products
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_orders_updated_at      BEFORE UPDATE ON sales.orders
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_order_items_updated_at BEFORE UPDATE ON sales.order_items
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_payments_updated_at    BEFORE UPDATE ON sales.payments
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
