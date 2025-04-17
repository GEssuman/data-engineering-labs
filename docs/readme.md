## Inventory & Order Management System

This project demonstrates an inventory and order management system using **PostgreSQL**. It includes the creation of key tables such as `customers`, `products`, `orders`, `order_details`, and `inventory_log`. 

### Key Features:
- **Stored procedures** to handle order processing as a transaction block
- **Triggers** to maintain data integrity and automate updates
- **Views** for simplified data analysis and reporting
- **Inventory logging** to track product changes and monitor order status


### Performace Optimization
- Added indexes on frequetly queried columns such as customer_id, product_id, ordered_id to reduce query execution time
- Converted selected views to materialized views where the data doesn't change frequently. This enhances read peerformance.

**ERD (Entity Relationship Diagram)** is also provided below to illustrate the database structure and relationships.
![](./ER-Diagram.png)



##  Database Setup With Docker
Step 1: Pull the PostgreSQL Image and Run the Container

``` bash
    docker run --name inventory_db \
  -e POSTGRES_DB=inventory_system \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres

```

Step 2: Connect with pgAdmin or DB Client

- Host: localhost
- Port: 5432
- Username: postgres
- Password: postgres
- Database: inventory_system

You can now import your SQL schema and run queries via pgAdmin or DBeaver