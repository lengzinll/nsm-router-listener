import re

def parse_mikrotik_log(raw, addr):

    log = { "source_device_ip": addr }
    priority = re.search(r"<(\d+)>",raw)

    if priority:
        log["priority"] = int(priority.group(1))

    # Remove syslog priority
    clean = re.sub(r"<\d+>", "",raw)

    # Header
    header = re.match(r"(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+(.*)",clean)
    if header:
        log["timestamp"] = header.group(1)
        log["router"] = header.group(2)
        message = header.group(3)
    else:
        message = clean

    # Action
    action = re.search(r"(\w+):",message)

    if action:
        log["action"] = action.group(1)

    # Interfaces
    incoming = re.search(
        r"in:<([^>]+)>",
        message
    )

    if incoming:
        log["incoming_interface"] = incoming.group(1)

    outgoing = re.search(
        r"out:([^,\s]+)",
        message
    )

    if outgoing:
        log["outgoing_interface"] = outgoing.group(1)

    # Connection state
    state = re.search(
        r"connection-state:(\w+)",
        message
    )

    if state:
        log["connection_state"] = state.group(1)

    # Protocol
    proto = re.search(
        r"proto\s+(\w+)",
        message
    )

    if proto:
        log["protocol"] = proto.group(1)


    # TCP flags
    flag = re.search(
        r"\((.*?)\)",
        message
    )

    if flag:
        log["flags"] = flag.group(1)

    # Source -> Destination
    connection = re.search(
        r"(\d+\.\d+\.\d+\.\d+):(\d+)->(\d+\.\d+\.\d+\.\d+):(\d+)",
        message
    )

    if connection:

        log["source_ip"] = connection.group(1)
        log["source_port"] = int(connection.group(2))
        log["destination_ip"] = connection.group(3)
        log["destination_port"] = int(connection.group(4))

    # Packet length
    length = re.search(
        r"len\s+(\d+)",
        message
    )

    if length:
        log["packet_length"] = int(length.group(1))


    return log