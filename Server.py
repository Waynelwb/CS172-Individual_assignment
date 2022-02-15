import pickle
import socketserver
import struct
import sqlite3


class Finish(Exception):
    pass


class RequestHandler(socketserver.StreamRequestHandler):
    Call = dict(
        NEWP=(
            lambda self, *args: self.new_product(*args)
        ),
        NEWS=(
            lambda self, *args: self.new_store(*args)
        ),
        INCREMENT=(
            lambda self, *args: self.increment(*args)
        ),
        DETAILS=(
            lambda self, *args: self.get_details(*args)
        ),
        SHUTDOWN=lambda self, *args: self.shutdown(*args)
    )

    def handle(self):
        size_struct = struct.Struct("!I")
        size_data = self.rfile.read(size_struct.size)
        size = size_struct.unpack(size_data)[0]
        data = pickle.loads(self.rfile.read(size))
        try:
            function = self.Call[data[0]]
            reply = function(self, *data[1:])
        except Finish:
            return
        data = pickle.dumps(reply, 3)
        self.wfile.write(size_struct.pack(len(data)))
        self.wfile.write(data)

    def get_product(self, product_name, need_exist=None):
        conn = sqlite3.connect("stock_management.db")
        c = conn.cursor()
        exam = c.execute("SELECT EXISTS (SELECT * FROM PRODUCT WHERE product_name = ?)",
                         (product_name,))
        for result in exam:
            if result[0] != need_exist:
                conn.close()
                return True

    def get_store(self, city, need_exist=None):
        conn = sqlite3.connect("stock_management.db")
        c = conn.cursor()
        exam = c.execute("SELECT EXISTS (SELECT * FROM STORE WHERE store_name = ?)",
                         (city,))
        for result in exam:
            if result[0] != need_exist:
                conn.close()
                return True

    def new_product(self, product_name, sell_price):
        if self.get_product(product_name, need_exist=0):
            return False, 'The product already exist!'
        conn = sqlite3.connect("stock_management.db")
        c = conn.cursor()
        count = 0
        for item in c.execute("SELECT COUNT(*) FROM (SELECT * FROM PRODUCT)"):
            count = item[0]
        count += 1
        c.execute("INSERT INTO PRODUCT (product_id, product_name, sell_price) VALUES (?, ?, ?)", (count, product_name,
                                                                                                  sell_price))
        conn.commit()
        conn.close()
        return True, 'Success!'

    def new_store(self, city):
        if self.get_store(city, need_exist=0):
            return False, "The store already exist!"
        conn = sqlite3.connect("stock_management.db")
        c = conn.cursor()
        count = 0
        for item in c.execute("SELECT COUNT(*) FROM (SELECT * FROM STORE)"):
            count = item[0]
        count += 1
        c.execute("INSERT INTO STORE (store_id, store_name) VALUES (?, ?)", (count, city))
        conn.commit()
        conn.close()
        return True, 'Success!'

    def increment(self, product_name, city, stock):
        if self.get_product(product_name, need_exist=1):
            return False, "The product doesn't exist!"
        if self.get_store(city, need_exist=1):
            return False, "The store doesn't exist!"
        conn = sqlite3.connect("stock_management.db")
        c = conn.cursor()
        store_id_data = c.execute("SELECT store_id FROM STORE WHERE store_name = ?", (city,))
        for item1 in store_id_data:
            store_id = item1[0]
            product_id_data = c.execute("SELECT product_id FROM PRODUCT WHERE product_name = ?", (product_name,))
            for item2 in product_id_data:
                product_id = item2[0]
                exam = c.execute("SELECT EXISTS (SELECT * FROM STOCK WHERE product_id = ? AND store_id = ?)",
                                 (product_id, store_id))
                for item3 in exam:
                    if item3[0] == 0:
                        count = 0
                        for number in c.execute("SELECT COUNT(*) FROM (SELECT * FROM STOCK)"):
                            count = number[0]
                        count += 1
                        c.execute("INSERT INTO STOCK (id, product_id, store_id, stock) VALUES (?, ?, ?, ?)",
                                  (count, product_id, store_id, stock))
                    else:
                        c.execute("UPDATE STOCK set stock = stock + ? WHERE product_id = ? AND store_id = ?",
                                  (stock, product_id, store_id))
        conn.commit()
        conn.close()
        return True, 'Success!'

    def get_details(self, product_name, city):
        if self.get_product(product_name, need_exist=1):
            return False, "The product doesn't exist!"
        if self.get_store(city, need_exist=1):
            return False, "The store doesn't exist!"
        conn = sqlite3.connect('stock_management.db')
        c = conn.cursor()
        store_id_data = c.execute("SELECT store_id FROM STORE WHERE store_name = ?", (city,))
        for item1 in store_id_data:
            store_id = item1[0]
            product_id_data = c.execute("SELECT product_id FROM PRODUCT WHERE product_name = ?", (product_name,))
            for item2 in product_id_data:
                product_id = item2[0]
                exam = c.execute("SELECT EXISTS (SELECT * FROM STOCK WHERE product_id = ? AND store_id = ?)",
                                 (product_id, store_id))
                for item3 in exam:
                    if item3[0] == 0:
                        conn.close()
                        return False, "Record doesn't exist!"
                    else:
                        stock = c.execute('SELECT * FROM STOCK WHERE store_id = ? AND product_id = ?',
                                          (store_id, product_id))
                        for data in stock:
                            conn.close()
                            return True, data[0], data[1], data[2], data[3]

    def shutdown(self, *ignore):
        self.server.shutdown()
        raise Finish


class StoreRegistrationServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def load(c, table):
    try:
        if table == 'STORE':
            c.execute('''CREATE TABLE IF NOT EXISTS STORE
            (store_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
            store_name TEXT UNIQUE NOT NULL);''')
        elif table == 'STOCK':
            c.execute('''CREATE TABLE IF NOT EXISTS STOCK
            (id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
            product_id INTEGER NOT NULL,
            store_id INTEGER NOT NULL,
            stock INTEGER);''')
        else:
            c.execute('''CREATE TABLE IF NOT EXISTS PRODUCT
            (product_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
            product_name TEXT UNIQUE NOT NULL,
            sell_price FLOAT NOT NULL);''')
    except EnvironmentError as err:
        pass


def main():
    conn = sqlite3.connect("stock_management.db")
    print("Connected to database successfully!")
    c = conn.cursor()
    for item in ["STORE", "STOCK", "PRODUCT"]:
        load(c, item)
    conn.commit()
    conn.close()
    server = None
    try:
        server = StoreRegistrationServer(("", 9653), RequestHandler)
        server.serve_forever()
    except Exception as err:
        print("ERROR", err)
    finally:
        if server is not None:
            server.shutdown()
            print("Close server successfully!")


main()
