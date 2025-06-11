import psycopg2
import random
from datetime import datetime, timedelta

# === CONFIGURATION ===
DB_CONFIG = {
    "dbname": "northwind",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": 5432
}

NUM_ORDERS = 100  # How many orders to insert

def get_last_order_id(cur):
    cur.execute('SELECT MAX("OrderID") FROM orders')
    result = cur.fetchone()
    return result[0] or 0  # If no rows, return 0

def insert_random_orders():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Fetch valid IDs
        cur.execute('SELECT "CustomerID" FROM customers')
        customer_ids = [row[0] for row in cur.fetchall()]
        cur.execute('SELECT "EmployeeID" FROM employees')
        employee_ids = [row[0] for row in cur.fetchall()]
        cur.execute('SELECT "ProductID", "UnitPrice" FROM products WHERE "Discontinued" = 0')
        products = cur.fetchall()

        cur.execute('SELECT distinct "ShipCountry" FROM orders')
        country = cur.fetchall()

        order_id = get_last_order_id(cur)
        print(f"Last OrderID: {order_id}")

        for _ in range(NUM_ORDERS):
            
            order_id = order_id + 1 
            print("Inserting order number:",order_id)
            cust_id = random.choice(customer_ids)
            emp_id = random.choice(employee_ids)
            country_code = random.choice(country)[0]
            order_date = datetime.now() - timedelta(days=random.randint(0, 30))
            required_date = order_date + timedelta(days=7)
            shipped_date = order_date + timedelta(days=random.randint(1, 5))
            ship_via = random.randint(1, 3)
            freight = round(random.uniform(10.0, 100.0), 2)
            # Insert into orders
            try:
                cur.execute("""
                    INSERT INTO orders ("OrderID","CustomerID", "EmployeeID", "OrderDate", "RequiredDate", "ShippedDate", "ShipVia", "Freight", "ShipCountry")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING "OrderID"
                """, (order_id, cust_id, emp_id, order_date, required_date, shipped_date, ship_via, freight, country_code))

                for _ in range(random.randint(1, 3)):
                    product_id, unit_price = random.choice(products)
                    quantity = random.randint(1, 10)
                    discount = round(random.uniform(0, 0.2), 2)

                    cur.execute("""
                        INSERT INTO order_details ("OrderID", "ProductID", "UnitPrice", "Quantity", "Discount")
                        VALUES (%s, %s, %s, %s, %s)
                    """, (order_id, product_id, unit_price, quantity, discount))
            except Exception as e:
                print(f"Error inserting order {order_id}: {e}")
                # If there's an error, skip to the next order
                conn.rollback()
                continue

        conn.commit()
        print(f"Inserted {NUM_ORDERS} random orders.")

    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    insert_random_orders()