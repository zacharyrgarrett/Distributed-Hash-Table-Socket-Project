# Zachary Garrett

import socket
import random
import pickle
import sys

echo_max = 255  # Max echo size
port_min = 26500  # Minimum allowed port
port_max = 26999  # Maximum allowed port

# Global Variables
state_info = dict()
dht_exists = False
accepting_requests = True
free_clients = 0
leader = ''
stoppage_requester = ''
dht_info = []


# Maintains the metadata and return code of a client request
class RInfo:
    attrs = dict(
        code='Default Code',
        msg='',
        added=dict()
    )

    def success(self):
        self.attrs["code"] = "SUCCESS"
        ret_val = self.get_attrs()
        self.set_msg("")
        return ret_val

    def failure(self):
        self.attrs["code"] = "FAILURE"
        ret_val = self.get_attrs()
        self.set_msg("")
        return ret_val

    def add_attr(self, name, value):
        self.attrs["added"][name] = value

    def get_attrs(self):
        return self.attrs

    def set_msg(self, message):
        self.attrs["msg"] = message


# Directs all commands
def handle_command(client_input):
    info = RInfo()
    global accepting_requests

    # Retrieve command elements
    elements = str.split(client_input, ' ')
    if len(elements) < 2:
        info.set_msg("Invalid input.")
        return info.failure()
    command = elements[0].replace("-", "_")
    params = elements[1:]

    if command in valid_commands and accepting_requests:        # Any requests are available
        return valid_commands[command](params)
    elif not accepting_requests and (command == "teardown-complete" or command == "dht_complete" or command == "dht_rebuilt"):  # Leader is done setting up
        return valid_commands[command](params)
    else:
        info.set_msg("Invalid command.")
        return info.failure()                                   # Command not allowed


# Register the new client
def register(params):
    info = RInfo()
    global free_clients
    if len(params) != 4:
        info.set_msg("Invalid number of parameters (expected 4)")
        return info.failure()

    # New element for the state information
    uname = params[0]
    client_info = dict(
        username=uname,
        ip=params[1],
        portl=params[2],
        portq=params[3],
        state="free"
    )

    # Check for duplicates
    if uname not in state_info:
        state_info[uname] = client_info
        free_clients += 1
        info.add_attr("client_info", client_info)
        return info.success()
    else:
        info.set_msg(f"{uname} already exists!")
        return info.failure()


# Deregister a client
def deregister(params):
    info = RInfo()
    global free_clients
    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    # Determine if element can be de-registered
    uname = params[0]
    if uname in state_info and state_info[uname]["state"] == "free":
        state_info.pop(uname)
        free_clients += -1
        return info.success()
    else:
        info.set_msg(f"Either {uname} does not exist or is not 'free'.")
        return info.failure()


# Setup DHT
def setup_dht(params):
    info = RInfo()
    global dht_exists
    if len(params) != 2 or dht_exists:
        info.set_msg("Invalid number of parameters (expected 2) OR the DHT already exists!")
        return info.failure()

    global free_clients
    global leader
    global accepting_requests
    global dht_info
    dht_size = int(params[0])
    uname = params[1]

    # Verify DHT requirements
    uname_exists = uname in state_info
    valid_dht_size = dht_size >= 2
    valid_num_users = len(state_info) >= 2
    valid_available_free = free_clients >= (dht_size - 1)

    # If requirements are met
    if uname_exists and valid_dht_size and valid_num_users and not dht_exists and valid_available_free:
        state_info[uname]["state"] = "leader"               # Assign leader
        leader = uname

        # Prepare information to pass to leader
        random_clients = rand_choose(dht_size - 1, "free")  # Get n-1 random clients
        assign_state(random_clients, "indht")               # Set random clients to InDHT
        tuples = get_tuples([uname] + random_clients)       # Retrieve tuples
        dht_info = tuples
        dht_exists = True
        accepting_requests = False

        # Add information to pass to the leader
        return_message = '\nDHT CLIENTS:'
        for tup in tuples:
            return_message += '\n' + str(tup)
        info.add_attr("message", return_message)
        info.add_attr("tuples", tuples)
        info.add_attr("dht_size", dht_size)

        return info.success()
    else:
        info.set_msg("The DHT cannot be set up at this time.")
        return info.failure()


# Randomly choose n clients that are in state s
def rand_choose(n, s):
    keys = list(state_info.keys())
    available_keys = list(range(0, len(keys)))
    chosen = []
    while len(chosen) < n:
        index = random.choice(available_keys)
        if state_info[keys[index]]["state"] == s:
            chosen.append(keys[index])
            available_keys.remove(index)
    return chosen


# Assigns the given state to the usernames provided
def assign_state(usernames, state):
    for uname in usernames:
        state_info[uname]["state"] = state


# Retrieves 4-tuples (username, ipv4, portl, portq)
def get_tuples(usernames):
    tuples = []
    for uname in usernames:
        client = state_info[uname]
        tuples.append((client["username"], client["ip"], client["portl"], client["portq"]))
    return tuples


# Complete DHT
def dht_complete(params):
    info = RInfo()
    global accepting_requests
    global leader

    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    # Check if uname is the leader
    uname = params[0]
    if uname == leader:
        accepting_requests = True   # Allow commands
        return info.success()
    else:
        info.set_msg(f"{uname} is not the Leader.")
        return info.failure()


# Query DHT
def query_dht(params):
    info = RInfo()
    global accepting_requests
    global dht_exists

    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    uname = params[0]

    # Determine if requirements are met
    if accepting_requests and dht_exists and uname in state_info and state_info[uname]["state"] == "free":
        rand_client = rand_choose(1, "indht")[0]            # Get random client in DHT
        client_tuple = get_tuples([rand_client])[0]         # Get tuple
        info.add_attr("tuple", client_tuple)                # Add tuple to return info
        return info.success()
    else:
        info.set_msg("A query cannot be made at this time.")
        return info.failure()


# Initiates a client leaving the DHT
def leave_dht(params):
    info = RInfo()
    global state_info

    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    uname = params[0]

    # Verify conditions
    uname_exists = uname in state_info
    valid_state = state_info[uname]["state"] != "free"
    global dht_exists, stoppage_requester, accepting_requests, dht_info

    index = -1
    for i in range(len(dht_info)):
        if dht_info[i][0] == uname:
            index = i
            break

    # Update info
    if index == len(dht_info) - 1:
        dht_info = dht_info[:index]
    elif index == 0:
        dht_info = dht_info[1:]
    else:
        dht_info = dht_info[0:index] + dht_info[index + 1:]

    # Add information to pass to the leader
    return_message = '\nDHT CLIENTS:'
    for tup in dht_info:
        return_message += '\n' + str(tup)
    info.add_attr("message", return_message)
    info.add_attr("tuples", dht_info)

    if uname_exists and valid_state and dht_exists:     # Allow client to move onto next steps
        stoppage_requester = uname
        # accepting_requests = False                      # Deny any incoming requests
        info.set_msg(f"Waiting on clients to verify {uname} has been removed from the DHT...")
        state_info[uname]["state"] = "free"
        return info.success()
    else:                                               # <username> cannot be removed
        info.set_msg(f"{uname} cannot be removed at this time.")
        print("Failed Hurr")
        print(uname_exists)
        print(valid_state)
        print(dht_exists)
        return info.failure()


# Initiates a client joining the DHT
def join_dht(params):
    info = RInfo()
    global state_info

    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    uname = params[0]

    # Verify conditions
    uname_exists = uname in state_info
    valid_state = uname_exists and state_info[uname]["state"] == "free"
    global dht_exists, stoppage_requester, accepting_requests, dht_info

    if valid_state and dht_exists:     # Allow client to move onto next steps
        stoppage_requester = uname
        # accepting_requests = False                      # Deny any incoming requests
        info.set_msg(f"Waiting on clients to verify {uname} has been inserted into the DHT...")
        dht_info.append(get_tuples([uname])[0])
        info.add_attr("tuples", dht_info)
        state_info[uname]["state"] = "indht"
        # Add information to pass to the leader
        return_message = '\nDHT CLIENTS:'
        for tup in dht_info:
            return_message += '\n' + str(tup)
        info.add_attr("message", return_message)
        return info.success()
    else:                                               # <username> cannot be removed
        info.set_msg(f"{uname} cannot be added at this time.")
        return info.failure()


def teardown_dht(params):
    info = RInfo()
    global leader
    global accepting_requests

    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    uname = params[0]

    # Verify the leader is requester the teardown
    if leader == uname:
        # accepting_requests = False
        info.set_msg(f"Waiting on clients to verify {uname} has been inserted into the DHT...")
        return info.success()
    else:
        info.set_msg(f"{uname} cannot request dht-teardown since they are not the leader.")
        return info.failure()


def teardown_complete(params):
    info = RInfo()
    global leader, accepting_requests, dht_exists, state_info

    if len(params) != 1:
        info.set_msg("Invalid number of parameters (expected 1)")
        return info.failure()

    uname = params[0]

    # Verify the leader has verified completion
    if leader == uname:
        accepting_requests = True
        dht_exists = False
        assign_state(state_info.keys(), "free")
        info.set_msg("DHT Teardown is successful.")
        return info.success()
    else:
        info.set_msg(f"{uname} is not the Leader and cannot confirm the DHT teardown.")
        return info.failure()


# Updates the state_info table with status of DHT after it was rebuilt
def dht_rebuilt(params):
    info = RInfo()
    global state_info
    global leader
    global accepting_requests

    if len(params) != 2:
        info.set_msg("Invalid number of parameters (expected 2)")
        return info.failure()

    uname = params[0]
    new_leader = params[1]

    # <username> must be the one that requested the leave
    global stoppage_requester
    if uname not in state_info or stoppage_requester != uname:
        info.set_msg(f"{uname} is not the user who initiated the DHT rebuild! It was {stoppage_requester}.")
        return info.failure()

    # Verify <new_leader> exists
    elif new_leader not in state_info:
        info.set_msg(f"{new_leader} does not exits!")
        return info.failure()

    # Free <username>, then assign the correct <new_leader>, then enable requests from clients
    else:
        # state_info[uname]["state"] = "free"
        if leader != new_leader:
            state_info[leader]["state"] = "indht"
            state_info[new_leader]["state"] = "leader"
            leader = new_leader
        accepting_requests = True
        return info.success()


# Used to map commands to the respective functions
valid_commands = dict(
    register=register,
    deregister=deregister,
    setup_dht=setup_dht,
    dht_complete=dht_complete,
    query_dht=query_dht,
    leave_dht=leave_dht,
    join_dht=join_dht,
    teardown_dht=teardown_dht,
    teardown_complete=teardown_complete,
    dht_rebuilt=dht_rebuilt
)

# Main
if __name__ == '__main__':

    # Retrieve arguments
    if len(sys.argv) == 2:
        PORT = int(sys.argv[1])
    else:
        PORT = -1

    # Set HOST and PORT numbers to listen on
    port_prompt = 'Input port to listen (' + str(port_min) + '-' + str(port_max) + '): '
    HOST = ''
    while not port_min <= PORT <= port_max:
        print("Port " + str(PORT) + " is invalid")
        PORT = int(input(port_prompt))

    # Establish socket and bind
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))

    while True:
        # Receive message
        msg, addr = sock.recvfrom(65535)
        client_command = msg.decode('utf-8')
        clientMsg = f"Message from client: {client_command}"
        clientIP = f"Client IP Address: {addr}"

        # Determine what to do with command
        command_response = handle_command(client_command)
        msg_to_send = pickle.dumps(command_response)

        # Print messages
        print("\n" + clientMsg)
        print(clientIP)
        print("Status: " + str(command_response["code"]))

        # Reply to client
        sock.sendto(msg_to_send, addr)
        del msg_to_send
