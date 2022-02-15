"""
new_(p)roduct new_(S)tore (i)ncrement (d)etails (s)top_server (q)uit；
"""
import collections
import pickle
import socket
import struct
import sys
import Console

Address = ['localhost', 9653]
StoreTuple = collections.namedtuple('StoreTuple', 'store_id store_name')
ProductTuple = collections.namedtuple('ProductTuple', 'product_id product_name sell_price')
StockTuple = collections.namedtuple('StockTuple', 'id product_id store_id stock')


class SocketManager:

    def __init__(self, address):
        self.address = address

    def __enter__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.address)
        return self.sock

    def __exit__(self, *ignore):
        self.sock.close()


def main():
    if len(sys.argv) > 1:
        Address[0] = sys.argv[1]
    call = dict(p=new_product, S=new_store, i=increment,
                d=get_details, s=stop_server, q=quit_client)
    menu = ("new_(p)roduct new_(S)tore (i)ncrement (d)etails "
            "(s)top_server (q)uit")
    valid = frozenset("Spidsq")
    while True:
        action = Console.get_menu_choice(menu, valid)
        call[action]()


def new_product():
    product_name = Console.get_string("Product name", "Product name")
    sell_price = Console.get_float("Sell price", "Sell price", minimum=0, allow_zero=False)
    ok, *data = handle_request("NEWP", product_name, sell_price)
    if not ok:
        print("The product already exists!")
    else:
        print("Product successfully created!")


def new_store():
    city = Console.get_string("City", "City")
    ok, *data = handle_request("NEWS", city)
    if not ok:
        print("The store already exists!")
    else:
        print("Store successfully created!")


def increment():
    product_name = Console.get_string("Product name", "Product name")
    city = Console.get_string("City", "City")
    stock = Console.get_integer("Stock", "Stock")
    ok, *data = handle_request("INCREMENT", product_name, city, stock)
    if not ok:
        print(data[0])
    else:
        print("Increase the stock of product successfully!")


def get_details():
    product_name = Console.get_string("Product name", "Product name")
    city = Console.get_string("City", "City")
    ok, *data = handle_request("DETAILS", product_name, city)
    if not ok:
        print(data[0])
    else:
        stock = StockTuple(*data)
        print("Stock: {0}".format(stock.stock))


def stop_server(*ignore):
    handle_request("SHUTDOWN", wait_for_reply=False)
    sys.exit()


def quit_client(*ignore):
    sys.exit()


def handle_request(*items, wait_for_reply=True):
    size_struct = struct.Struct("!I")
    data = pickle.dumps(items, 3)

    try:
        with SocketManager(tuple(Address)) as sock:
            sock.sendall(size_struct.pack(len(data)))
            sock.sendall(data)
            if not wait_for_reply:
                return

            size_data = sock.recv(size_struct.size)
            size = size_struct.unpack(size_data)[0]
            result = bytearray()
            while True:
                data = sock.recv(4000)
                if not data:
                    break
                result.extend(data)
                if len(result) >= size:
                    break
        return pickle.loads(result)
    except socket.error as err:
        print("{0}: is the server running?".format(err))
        sys.exit(1)


main()
