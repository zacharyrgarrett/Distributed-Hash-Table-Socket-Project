# Zachary Garrett

import socket
import pickle
import threading
import logging
import sys
from csv import reader
from time import sleep

echo_max = 255  # Max echo size
port_min = 26500  # Minimum allowed port
port_max = 26999  # Maximum allowed port

identifier = -1
ring_size = -1

local_ip = ''
port_left = -1
port_query = -1

next_addr = ('', 0)

hash_table = [None] * 353
leader = ''
my_name = ''

dht_addresses = []


# Start threads to listen to the ports
def start_listening(params, cmd_params):
    global port_left, port_query, local_ip, my_name

    my_name = cmd_params[0]     # Save client nickname
    port_left = int(params["client_info"]["portl"])
    port_query = int(params["client_info"]["portq"])
    local_ip = params["client_info"]["ip"]
    left_thread = threading.Thread(target=listen_left, args=(port_left,))  # Config thread that listens to left
    query_thread = threading.Thread(target=listen_query, args=(port_query,))  # Config thread that listens to query
    left_thread.start()  # Start thread
    query_thread.start()  # Start thread


# Setup the node
def setup_node(params):
    global next_addr, ring_size, identifier, dht_addresses

    next_addr = params["next"]
    ring_size = params["ring_size"]
    identifier = params["identifier"]
    dht_addresses = params["all_addresses"]


# Decides if row should be stored in this node
# If not, send it to the next node
def handle_store_row(params):
    global identifier, hash_table, next_addr, ring_size

    row = params["data_row"]
    node_id = row["node_id"]
    pos = row["pos"]

    # Should be stored in current DHT client
    if node_id == identifier:
        # print("Data record id (" + str(node_id) + ") matches! Storing record in hash table position " + str(pos))
        row.pop("node_id")
        row.pop("pos")
        hash_table[pos] = row

    # Should not be stored in current DHT client, send to the next one
    else:
        # print("\nData record id (" + str(node_id) + ") does not match this client's id (" + str(
        #     identifier) + ")\nNow sending to client " + str(next_addr) + "...\n")
        send(params, next_addr)


# Logs message
def log_message(params):
    print(params["message"] + "\n")


# Decides if query can be handled by current node
def check_query_status(params):
    global identifier, next_addr, hash_table

    # Current DHT Client can handle the query
    if params["node_id"] == identifier:
        print("\nQuery identifier ("+str(identifier)+") matches!")
        return_msg = dict()

        # Record exists in hash table
        if hash_table[params["pos"]] is not None:
            print("Hash table has found a record in Position " + str(params["pos"]) + "! Sending record...")
            return_msg["code"] = "query_success"
            return_msg["message"] = "Data Record for " + params["long_name"] + ":\n" + str(hash_table[params["pos"]])

        # Record does not exist in hash table
        else:
            print("Hash table does not contain any records in Position " + str(params["pos"]) + "\n")
            return_msg["code"] = "query_failed"
            return_msg["message"] = "Record associated with '" + params["long_name"] + "' is not found in DHT"
        send(return_msg, params["return_addr"])

    # Send to next DHT client
    else:
        print("\nQuery record id (" + str(params["node_id"]) + ") does not match this client's id (" + str(
            identifier) + ")\nNow sending to client " + str(next_addr) + "...\n")
        send(params, next_addr)


# Removes current node's dht info and then passes on teardown to the next node
def teardown_dht(params):
    global leader, my_name, next_addr

    if leader == my_name:
        print("\nI am the leader. Deleting DHT information...")
        reset_dht_globals()
        print("Confirming teardown-complete with the server...")
        send("teardown-complete " + str(my_name), params["serverAddr"], True)
        print("Teardown confirmed.")
    else:
        print("\nTeardown DHT --> Deleting DHT information...")
        send(params, next_addr)     # Pass 'teardown' to next node
        reset_dht_globals()
        print("Successfully deleted local DHT info.")


# Reset global DHT values
def reset_dht_globals():
    global leader, my_name, next_addr, ring_size, identifier
    leader = ''  # Remove all dht information
    ring_size = -1
    identifier = -1
    next_addr = ('', 0)



def leave_dht(params):
    global dht_addresses, identifier

    print("BEFORE")
    print(dht_addresses)
    dht_addresses = params["tuples"]

    # if identifier == len(dht_addresses) - 1:
    #     dht_addresses = dht_addresses[:identifier]
    # elif identifier == 0:
    #     dht_addresses = dht_addresses[1:]
    # else:
    #     dht_addresses = dht_addresses[0:identifier] + dht_addresses[identifier+1:]

    print("AFTER")
    print(dht_addresses)

    params["dht_size"] = len(dht_addresses)
    config_dht_users(params, [])

    send("dht-rebuilt " + str(params["original"]) + " " + str(dht_addresses[0][0]), params["serverAddr"], True)


# Handles incoming requests for left port
left_handler = dict(
    setup=setup_node,
    store_row=handle_store_row,
    query=check_query_status,
    teardown=teardown_dht,
    leave=leave_dht
)


# Listens to specified port in infinite loop
def listen_left(port):
    # Establish socket and bind
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', port))
    sock.setblocking(False)

    while True:
        try:
            msg, from_addr = sock.recvfrom(65535)

            if msg is not None:
                params = pickle.loads(msg)

                # Call instructions based on code
                if params["code"] in left_handler:
                    left_handler[params["code"]](params)
        # Nonblocking error
        except socket.timeout as e:
            err = e.args[0]
            if err == 'timed out':
                sleep(1)
                continue
            else:
                print(e)
                sys.exit(1)
        # Other error has occurred
        except socket.error as e:
            sleep(1)
            continue
            # print(e)
            # sys.exit(1)


# Handles incoming requests for query port
query_handler = dict(
    query=check_query_status,
    query_success=log_message,
    query_failed=log_message
)


# Listens to query port
def listen_query(port):
    # Establish socket and bind
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', port))
    sock.setblocking(False)

    while True:
        try:
            msg, from_addr = sock.recvfrom(65535)

            if msg is not None:
                params = pickle.loads(msg)

                # Call instructions based on code
                if params["code"] in query_handler:
                    query_handler[params["code"]](params)
        # Nonblocking error
        except socket.timeout as e:
            err = e.args[0]
            if err == 'timed out':
                sleep(1)
                continue
            else:
                print(e)
                sys.exit(1)
        # Other error has occurred
        except socket.error as e:
            sleep(1)
            continue
            # print(e)
            # sys.exit(1)


# Send message to the right
def send(content, path_info, str_val=False):
    path_info = (path_info[0], int(path_info[1]))

    if str_val:
        send_msg = content.encode('utf-8')
    else:
        send_msg = pickle.dumps(content)

    # Establish socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Send
    sock.sendto(send_msg, path_info)


# Leader sends each client their mapping to the next client in the path
def config_dht_users(params, cmd_params):
    global next_addr, identifier, ring_size, leader, dht_addresses

    client_addresses = params["tuples"]
    dht_addresses = client_addresses
    leader_name = client_addresses[0][0]
    leader = leader_name

    # print("Client Addresses")
    # print(client_addresses)
    next_addr = (client_addresses[1][1], client_addresses[1][2])  # Set leader's next address
    identifier = 0  # Set leader's identifier
    ring_size = params["dht_size"]

    # Send each DHT client their pathing for the DHT cycle
    for i in range(1, len(client_addresses)):
        receiver = (client_addresses[i][1], client_addresses[i][2])
        if i == len(client_addresses) - 1:
            send_addr = (client_addresses[0][1], client_addresses[0][2])    # Sends to client left port
        else:
            send_addr = (client_addresses[i + 1][1], client_addresses[i + 1][2])    # Sends to client left port
        data = dict(code="setup", next=send_addr, ring_size=params["dht_size"], identifier=i, all_addresses=dht_addresses)
        send(data, receiver)

    store_data()
    print(params["message"])
    send("dht-complete " + leader_name, params["serverAddr"], True)


# Stores csv data into the DHT
def store_data():
    global next_addr
    data = read_from_csv()
    for row in data:
        inputs = dict(code="store_row", data_row=row)
        send(inputs, next_addr)


# Reads csv into list
def read_from_csv():
    data = []
    with open('StatsCountry.csv', 'r', encoding='utf-8', errors='ignore') as read_obj:
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        for row in csv_reader:
            assoc_row = dict()
            for i in range(len(header)):
                assoc_row[header[i]] = row[i]  # Make associative
            hash_vals = compute_hash(assoc_row)  # Compute hash
            assoc_row["pos"] = hash_vals["pos"]
            assoc_row["node_id"] = hash_vals["node_id"]
            data.append(assoc_row)  # Add to data list
    return data


# Computes the hash function
def compute_hash(row):
    long_name = row["Long Name"]
    total_ascii = 0
    for letter in long_name:
        total_ascii += ord(letter)
    pos = total_ascii % 353  # Compute pos
    node_id = pos % ring_size  # Compute node identifier
    return dict(pos=pos, node_id=node_id)


# Sends query to client in DHT
def submit_query(params, cmd_params):
    global port_query, local_ip
    long_name = input("Enter long name to query: ")
    hash_input = dict()
    hash_input["Long Name"] = long_name
    hash_output = compute_hash(hash_input)

    # Define Information that will be forwarded in the cycle
    forward_information = dict()
    forward_information["code"] = "query"
    forward_information["long_name"] = long_name
    forward_information["pos"] = hash_output["pos"]
    forward_information["node_id"] = hash_output["node_id"]
    forward_information["return_addr"] = (local_ip, port_query)

    # Get query addr
    query_addr = (params["tuple"][1], params["tuple"][3])

    # Send query
    send(forward_information, query_addr)
    sleep(3)


# Starts the teardown process. This will always be started by the leader.
def initiate_teardown(params, cmd_params):
    global next_addr
    params["code"] = "teardown"
    send(params, next_addr)


def initiate_leave(params, cmd_params):
    global next_addr, my_name
    params["code"] = "leave"
    params["original"] = my_name
    send(params, next_addr)
    reset_dht_globals()
    print("\nThis client has successfully been removed from the DHT.")


def join_dht(params, cmd_params):
    global dht_addresses, identifier, my_name, local_ip, port_left, port_query

    print("BEFORE")
    print(dht_addresses)
    dht_addresses = params["tuples"]
    print("AFTER")
    print(dht_addresses)

    # Add address
    identifier = len(dht_addresses)
    # dht_addresses.append((my_name, local_ip, port_left, port_query))

    params["tuples"] = dht_addresses
    params["dht_size"] = len(dht_addresses)
    config_dht_users(params, [])
    print("\nThis client has successfully been added to the DHT.")

    send("dht-rebuilt " + str(my_name) + " " + str(dht_addresses[0][0]), params["serverAddr"], True)


# Commands that need additional instructions
special_instructions = dict(
    setup_dht=config_dht_users,
    register=start_listening,
    query_dht=submit_query,
    teardown_dht=initiate_teardown,
    leave_dht=initiate_leave,
    join_dht=join_dht
)

if __name__ == '__main__':

    # Get arguments for IPv4 and port
    format_log = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format_log, level=logging.INFO,
                        datefmt="%H:%M:%S")

    ip_prompt = 'Input Server IPv4: '
    port_prompt = 'Input Server port (' + str(port_min) + '-' + str(port_max) + '): '

    if len(sys.argv) == 3:
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])
    else:
        HOST = str(input(ip_prompt))
        PORT = int(input(port_prompt))

    # Set HOST and PORT numbers to listen on
    while not port_min <= PORT <= port_max:
        print("Port " + str(PORT) + " is invalid")
        PORT = int(input(port_prompt))

    while True:
        sleep(1)

        # Get command
        command = str(input())
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.sendto(command.encode('utf-8'), (HOST, PORT))

        # Get response
        serverMsg, serverAddr = client_sock.recvfrom(65535)

        info = pickle.loads(serverMsg)
        print("Status: " + info["code"]) # + "\n" + info["msg"] + "\n")
        # print("Additional Info: " + str(info["added"]))

        # Determine if further instructions are needed
        command_attrs = command.split(" ")
        basic_command = command_attrs[0].replace("-", "_")
        command_params = command_attrs[1:]
        if basic_command in special_instructions and info["code"] == "SUCCESS":
            info["added"]["serverAddr"] = serverAddr
            special_instructions[basic_command](info["added"], command_params)
