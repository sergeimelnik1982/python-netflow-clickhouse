#!/usr/bin/env python3

HOST, PORT = '0.0.0.0', 9996

pid='/tmp/python-flowcollector.pid'
clickhouse_host = '127.0.0.1'
user = 'pynetflow'

import socketserver, threading, struct, socket
from clickhouse_driver.client import Client
from datetime import datetime, date, time
from daemonize import Daemonize

def unpackbufferint(buff, pointer, size):
    if size == 1:
        return struct.unpack('!B', buff[pointer:pointer + size])[0]
    if size == 2:
        return struct.unpack('!H', buff[pointer:pointer + size])[0]
    if size == 4:
        return struct.unpack('!I', buff[pointer:pointer + size])[0]
    else:
        print('Invalid integer size: %i' % size)

class SyslogUDPHandler(socketserver.BaseRequestHandler):

    def handle(self):
        flowpacket = self.request[0]
        timestamp=int(round(datetime.utcnow().timestamp() * 1000))
        (version, recordscount) = struct.unpack('!HH',flowpacket[0:4])
        if version == 5:
            for recordcounter in range(0, recordscount):
                recordpointer = 24 + (recordcounter * 48)
                srcaddr = socket.inet_ntoa(flowpacket[recordpointer:recordpointer + 4])
                dstaddr = socket.inet_ntoa(flowpacket[recordpointer + 4:recordpointer + 8])
#                nxthp = socket.inet_ntoa(flowpacket[recordpointer + 8:recordpointer + 12])
#                dPkts = unpackbufferint(flowpacket, recordpointer + 16, 4)
#                dOctets = unpackbufferint(flowpacket, recordpointer + 20, 4)
#                startflow = unpackbufferint(flowpacket, recordpointer + 24, 4)
#                endflow = unpackbufferint(flowpacket, recordpointer + 28, 4)
                srcport = unpackbufferint(flowpacket, recordpointer + 32, 2)
                dstport = unpackbufferint(flowpacket, recordpointer + 34, 2)
                l4proto = unpackbufferint(flowpacket, recordpointer + 38, 1)

                ClickHouse.execute("INSERT INTO NETFLOW.FLOWS (EventTimestamp,SrcIp,DstIp,Proto,SrcPort,DstPort) values", \
                    [{'EventTimestamp': timestamp, \
                    'SrcIp': srcaddr, \
                    'DstIp': dstaddr, \
                    'Proto': l4proto, \
                    'SrcPort': srcport, \
                    'DstPort': dstport }])

def main():
    server = socketserver.UDPServer((HOST,PORT), SyslogUDPHandler)
    server_thread = threading.Thread(target=server.serve_forever(poll_interval=0.5))
    server_thread.daemon = True
    server_thread.start()

daemon_params = {'app': 'syslog-server',
                 'pid': pid,
                 'action': main,
                 'user': 'pysyslog' }

ClickHouse = Client(clickhouse_host)

daemon = Daemonize(**daemon_params)

daemon.start()

