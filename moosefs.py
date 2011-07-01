#!/usr/bin/env python2
'''
A Python library for gathering information from MooseFS
'''
import socket
import struct
import time
import traceback
import sys

masterhost = 'mfsmaster'
masterport = 9421
mastername = 'MooseFS'

def mysend(socket,msg):
    totalsent = 0
    while totalsent < len(msg):
        sent = socket.send(msg[totalsent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent 

def myrecv(socket,leng):
    msg = '' 
    while len(msg) < leng:
        chunk = socket.recv(leng-len(msg))
        if chunk == '':
            raise RuntimeError("socket connection broken")
        msg = msg + chunk
    return msg

# check version
masterversion = (0,0,0)
try:
    s = socket.socket()
    s.connect((masterhost,masterport))
    mysend(s,struct.pack(">LL",510,0))
    header = myrecv(s,8)
    cmd,length = struct.unpack(">LL",header)
    data = myrecv(s,length)
    if cmd==511:
        if length==52:
            masterversion = (1,4,0)
        elif length==60:
            masterversion = (1,5,0)
        elif length==68:
            masterversion = struct.unpack(">HBB",data[:4])
except:
    pass

def servers_data():
    out = []
    s = socket.socket()
    s.connect((masterhost,masterport))
    mysend(s,struct.pack(">LL",500,0))
    header = myrecv(s,8)
    cmd,length = struct.unpack(">LL",header)
    if masterversion>=(1,5,13) and (length%54)==0:
        data = myrecv(s,length)
        n = length/54
        servers = []
        for i in xrange(n):
            d = data[i*54:(i+1)*54]
            v1,v2,v3,ip1,ip2,ip3,ip4,port,used,total,chunks,tdused,tdtotal,tdchunks,errcnt = struct.unpack(">HBBBBBBHQQLQQLL",d)
            try:
                host = (socket.gethostbyaddr("%u.%u.%u.%u" % (ip1,ip2,ip3,ip4)))[0]
            except Exception:
                host = "(unresolved)"
            ip = '.'.join([str(ip1), str(ip2), str(ip3), str(ip4)])
            ver = '.'.join([str(v1), str(v2), str(v3)])
            servers.append({
                'host':      host,
                'ip':        ip,
                'version':   ver,
                'port':      port,
                'used':      used,
                'total':     total,
                'chunks':    chunks,
                'tdused':    tdused,
                'tdtotal':   tdtotal,
                'tdchucnks': tdchunks,
                'errcnt':    errcnt,
            })
            # This function only returns raw data, in bytes.
            # The Web UI also includes:
            # regular hdd space, used (human readable) (based on used)
            # regular hdd space, total (human readable) (based on total)
            # regular hdd space, percent used (int((used*200.0)/total),(used*100.0)/total))
            # marked for removal hdd space, used (human readable) (based on tdused)
            # marked for removal hdd space, total (human readable) (based on tdtotal)
            # marked for removal hdd space, percent used (int((tdused*200.0)/tdtotal),(tdused*100.0)/tdtotal))
        print servers
        s.close()


servers_data()
sys.exit(0)
