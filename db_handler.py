from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    cur.execute("SELECT MAX(i_item_sk) FROM item")
    max_sk = cur.fetchone()[0]
    new_sk = (max_sk + 1) if max_sk is not None else 1

    rec_start_date = f"{new_item.start_year}-01-01"

    query = """
        INSERT INTO item (i_item_sk, i_item_id, i_rec_start_date, i_product_name, 
                          i_brand, i_category, i_manufact, i_current_price, i_num_owned)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (new_sk, new_item.item_id, rec_start_date, new_item.product_name,
              new_item.brand, new_item.category, new_item.manufact, 
              new_item.current_price, new_item.num_owned)
    
    cur.execute(query, params)


def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    name_parts = new_customer.name.split(' ', 1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    try:
        street_num, rest = new_customer.address.split(' ', 1)
        street_name, rest = rest.split(',', 1)
        city, rest = rest.split(',', 1)
        state_zip = rest.strip().split(' ')
        state = state_zip[0]
        zip_code = state_zip[1] if len(state_zip) > 1 else ""
    except ValueError:
        street_num, street_name, city, state, zip_code = "", "", "", "", ""

    cur.execute("SELECT MAX(ca_address_sk) FROM customer_address")
    max_addr_sk = cur.fetchone()[0]
    new_addr_sk = (max_addr_sk + 1) if max_addr_sk is not None else 1

    addr_query = """
        INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, 
                                      ca_city, ca_state, ca_zip)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    cur.execute(addr_query, (new_addr_sk, street_num, street_name.strip(), 
                             city.strip(), state, zip_code))

    cur.execute("SELECT MAX(c_customer_sk) FROM customer")
    max_cust_sk = cur.fetchone()[0]
    new_cust_sk = (max_cust_sk + 1) if max_cust_sk is not None else 1

    cust_query = """
        INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, 
                              c_last_name, c_email_address, c_current_addr_sk)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    cur.execute(cust_query, (new_cust_sk, new_customer.customer_id, first_name, 
                             last_name, new_customer.email, new_addr_sk))


def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    if new_customer.name or new_customer.email:
        update_parts = []
        params = []
        
        if new_customer.name:
            name_parts = new_customer.name.split(' ', 1)
            update_parts.append("c_first_name = ?, c_last_name = ?")
            params.extend([name_parts[0], name_parts[1] if len(name_parts) > 1 else ""])
        
        if new_customer.email:
            update_parts.append("c_email_address = ?")
            params.append(new_customer.email)
            
        params.append(original_customer_id)
        cur.execute(f"UPDATE customer SET {', '.join(update_parts)} WHERE c_customer_id = ?", params)

    if new_customer.address:
        cur.execute("SELECT c_current_addr_sk FROM customer WHERE c_customer_id = ?", (original_customer_id,))
        addr_sk = cur.fetchone()[0]
        
        try:
            parts = new_customer.address.split(' ', 1)
            num = parts[0]
            rest = parts[1].split(',', 2)
            name = rest[0].strip()
            city = rest[1].strip()
            state_zip = rest[2].strip().split(' ')
            state, zip_c = state_zip[0], state_zip[1]
            
            addr_update = """
                UPDATE customer_address 
                SET ca_street_number = ?, ca_street_name = ?, ca_city = ?, ca_state = ?, ca_zip = ?
                WHERE ca_address_sk = ?
            """
            cur.execute(addr_update, (num, name, city, state, zip_c, addr_sk))
        except (ValueError, IndexError):
            pass 


def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    rental_date = date.today()
    due_date = rental_date + timedelta(days=14)
    
    query = "INSERT INTO rental (item_id, customer_id, rental_date, due_date) VALUES (?, ?, ?, ?)"
    cur.execute(query, (item_id, customer_id, rental_date, due_date))


def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    new_pos = line_length(item_id) + 1
    
    query = "INSERT INTO waitlist (item_id, customer_id, place_in_line) VALUES (?, ?, ?)"
    cur.execute(query, (item_id, customer_id, new_pos))
    
    return new_pos

def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    cur.execute("DELETE FROM waitlist WHERE item_id = ? AND place_in_line = 1", (item_id,))
    
    cur.execute("UPDATE waitlist SET place_in_line = place_in_line - 1 WHERE item_id = ?", (item_id,))


def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    cur.execute("SELECT rental_date, due_date FROM rental WHERE item_id = ? AND customer_id = ?", 
                (item_id, customer_id))
    rental_info = cur.fetchone()
    
    if rental_info:
        rental_date, due_date = rental_info
        return_date = date.today()
        
        insert_history = """
            INSERT INTO rental_history (item_id, customer_id, rental_date, due_date, return_date) 
            VALUES (?, ?, ?, ?, ?)
        """
        cur.execute(insert_history, (item_id, customer_id, rental_date, due_date, return_date))
        
        cur.execute("DELETE FROM rental WHERE item_id = ? AND customer_id = ?", 
                    (item_id, customer_id))


def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    cur.execute("SELECT due_date FROM rental WHERE item_id = ? AND customer_id = ?", 
                (item_id, customer_id))
    result = cur.fetchone()
    
    if result:
        new_due_date = result[0] + timedelta(days=14)
        cur.execute("UPDATE rental SET due_date = ? WHERE item_id = ? AND customer_id = ?", 
                    (new_due_date, item_id, customer_id))


def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    query = """SELECT i_item_id, i_product_name, i_brand, i_category, 
                      i_manufact, i_current_price, YEAR(i_rec_start_date), i_num_owned 
               FROM item WHERE 1=1"""
    params = []
    op = "LIKE" if use_patterns else "="

    if filter_attributes.item_id:
        query += f" AND i_item_id {op} ?"
        params.append(filter_attributes.item_id)
    if filter_attributes.product_name:
        query += f" AND i_product_name {op} ?"
        params.append(filter_attributes.product_name)
    if filter_attributes.brand:
        query += f" AND i_brand {op} ?"
        params.append(filter_attributes.brand)
    if filter_attributes.category:
        query += f" AND i_category {op} ?"
        params.append(filter_attributes.category)
    if filter_attributes.manufact:
        query += f" AND i_manufact {op} ?"
        params.append(filter_attributes.manufact)

    if min_price != -1:
        query += " AND i_current_price >= ?"
        params.append(min_price)
    if max_price != -1:
        query += " AND i_current_price <= ?"
        params.append(max_price)
    if min_start_year != -1:
        query += " AND YEAR(i_rec_start_date) >= ?"
        params.append(min_start_year)
    if max_start_year != -1:
        query += " AND YEAR(i_rec_start_date) <= ?"
        params.append(max_start_year)

    cur.execute(query, params)
    
    return [Item(row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip(), 
                 row[4].strip(), float(row[5]), int(row[6]), int(row[7])) 
            for row in cur.fetchall()]


def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """
    params = []
    op = "LIKE" if use_patterns else "="

    if filter_attributes.customer_id:
        query += f" AND c.c_customer_id {op} ?"
        params.append(filter_attributes.customer_id)
    if filter_attributes.name:
        query += f" AND CONCAT(c.c_first_name, ' ', c.c_last_name) {op} ?"
        params.append(filter_attributes.name)
    if filter_attributes.email:
        query += f" AND c.c_email_address {op} ?"
        params.append(filter_attributes.email)
    if filter_attributes.address:
        address_concat = "CONCAT(ca.ca_street_number, ' ', ca.ca_street_name, ', ', ca.ca_city, ', ', ca.ca_state, ' ', ca.ca_zip)"
        query += f" AND {address_concat} {op} ?"
        params.append(filter_attributes.address)

    cur.execute(query, params)

    return [Customer(row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()) 
            for row in cur.fetchall()]


def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    query_owned = "SELECT i_num_owned FROM item WHERE i_item_id = ?"
    cur.execute(query_owned, (item_id,))
    result = cur.fetchone()
    
    if result is None:
        return -1
    
    num_owned = result[0]

    query_rented = "SELECT COUNT(*) FROM rental WHERE item_id = ?"
    cur.execute(query_rented, (item_id,))
    num_rented = cur.fetchone()[0]

    return num_owned - num_rented


def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    query = "SELECT place_in_line FROM waitlist WHERE item_id = ? AND customer_id = ?"
    cur.execute(query, (item_id, customer_id))
    result = cur.fetchone()
    return result[0] if result else -1


def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    query = "SELECT COUNT(*) FROM waitlist WHERE item_id = ?"
    cur.execute(query, (item_id,))
    result = cur.fetchone()
    return result[0] if result else 0


def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    cur.close()
    conn.close()

