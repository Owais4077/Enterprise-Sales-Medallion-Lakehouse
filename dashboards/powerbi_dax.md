# Power BI Star Schema Model & DAX Metrics Specification

This document specifies the Data Model relationships and production DAX metrics for Power BI reporting on top of the Gold Delta Lake star schema.

---

## 📐 Star Schema Model Relationships

| Foreign Table & Key | Direction | Primary Table & Key | Cardinality |
|---|---|---|---|
| `fact_sales[order_date_key]` | $\rightarrow$ | `dim_date[date_key]` | Many-to-One (`*:1`) |
| `fact_sales[customer_id]` | $\rightarrow$ | `dim_customer[customer_id]` | Many-to-One (`*:1`) |
| `fact_sales[product_id]` | $\rightarrow$ | `dim_product[product_id]` | Many-to-One (`*:1`) |
| `fact_sales[shipping_country]` | $\rightarrow$ | `dim_country[country_code]` | Many-to-One (`*:1`) |

---

## 🧮 Core DAX Measures

### 1. Revenue & Sales Metrics

```dax
Total Sales USD = 
SUM(fact_sales[line_total_usd])
```

```dax
Total Sales Local = 
SUM(fact_sales[line_total_local])
```

```dax
Total Orders = 
DISTINCTCOUNT(fact_sales[order_id])
```

```dax
Total Items Sold = 
SUM(fact_sales[quantity])
```

```dax
Average Order Value USD = 
DIVIDE([Total Sales USD], [Total Orders], 0)
```

---

### 2. Time Intelligence & Performance Metrics

```dax
YTD Revenue USD = 
TOTALYTD([Total Sales USD], 'dim_date'[full_date])
```

```dax
Prior Year Revenue USD = 
CALCULATE(
    [Total Sales USD], 
    SAMEPERIODLASTYEAR('dim_date'[full_date])
)
```

```dax
YoY Revenue Growth % = 
DIVIDE(
    [Total Sales USD] - [Prior Year Revenue USD], 
    [Prior Year Revenue USD], 
    0
)
```

---

### 3. Customer & Portfolio Analytics

```dax
Active Customers = 
CALCULATE(
    DISTINCTCOUNT(fact_sales[customer_id]), 
    FILTER('dim_customer', 'dim_customer'[is_active] = TRUE)
)
```

```dax
Average Revenue Per Customer USD = 
DIVIDE([Total Sales USD], [Active Customers], 0)
```

