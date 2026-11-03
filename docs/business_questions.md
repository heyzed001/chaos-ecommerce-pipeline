# Business Questions the Data Marts Must Answer

## Objective
The purpose of the data marts is to transform raw e-commerce data into structured datasets that support business reporting and decision-making.

The data marts should answer the following key business questions.

### 1. Daily Revenue Analysis
- How much revenue does the business generate each day?
- How does daily revenue change over time?
- Which days have the highest and lowest revenue?
- How many orders are placed each day?
- What is the average order value (AOV)?

### 2. Revenue by Customer Region
- Which customer regions generate the highest revenue?
- How does revenue vary across regions?
- How many unique customers purchase from each region?
- What is the average order value by region?
- Which regions show increasing or declining sales?

### 3. Product Performance
- Which products generate the highest revenue?
- Which products sell the most units?
- What are the top 10 products by revenue?
- Which product categories contribute the most revenue?
- Which products have the lowest sales performance?

## Expected Data Mart Outputs

| Data Mart | Purpose |
|---|---|
| `mart_daily_sales` | Daily revenue, order counts, units sold and average order value |
| `mart_regional_sales` | Revenue, customers and order performance by region |
| `mart_product_performance` | Revenue, quantities sold and product rankings |

## Key Business Metrics

- **Revenue:** Total sales value after applicable discounts.
- **Order Count:** Number of distinct valid orders.
- **Units Sold:** Total quantity of products sold.
- **Average Order Value:** Revenue divided by the number of valid orders.
- **Unique Customers:** Number of distinct customers who placed valid orders.

## Business Rules to Define

Before building the marts, determine:

- Which order statuses count towards revenue?
- Should canceled orders be excluded?
- How should refunds and returns be treated?
- Which customer region should historical sales use when a customer changes address?
- How will duplicate orders be prevented from inflating revenue?
- How will late-arriving orders affect previously calculated daily sales?

## Success Criteria

- [ ] Calculate daily revenue accurately.
- [ ] Aggregate sales by customer region.
- [ ] Rank products by revenue and quantity sold.
- [ ] Prevent duplicate records from inflating sales.
- [ ] Preserve historical customer-region reporting.
- [ ] Produce consistent metrics across all three marts.