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
    s = socket.socket()
    s.connect((masterhost,masterport))
    mysend(s,struct.pack(">LL",500,0))
    header = myrecv(s,8)
    cmd,length = struct.unpack(">LL",header)
    if cmd==501 and masterversion >= (1,5,13) and (length%54)==0:
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

def info_data():
    # Basic info table
    s = socket.socket()
    s.connect((masterhost,masterport))
    mysend(s,struct.pack(">LL",510,0))
    header = myrecv(s,8)
    cmd,length = struct.unpack(">LL",header)
    info = {}
    if cmd==511 and length==68:
        data = myrecv(s,length)
        v1,v2,v3,total,avail,trspace,trfiles,respace,refiles,nodes,dirs,files,chunks,allcopies,tdcopies = struct.unpack(">HBBQQQLQLLLLLLL",data)
        ver = '.'.join([str(v1), str(v2), str(v3)])
        info = {
            'version':              ver,
            'total_space':          total,
            'avail_space':          avail,
            'trash_space':          trspace,
            'trash_files':          trfiles,
            'reserved_space':       respace,
            'reserved_files':       refiles,
            'all_fs_objects':       nodes,
            'directories':          dirs,
            'files':                files,
            'chunks':               chunks,
            'all_chunk_copies':     allcopies,
            'regular_chunk_copies': tdcopies,
        }
        print info
    s.close()

    # All chunks state matrix
    matrix = []
    if masterversion >= (1,5,13):
        # For INmatrix, 0 means all, 1 means regular
        INmatrix = 0
        s = socket.socket()
        s.connect((masterhost,masterport))
        if masterversion>=(1,6,10):
            mysend(s,struct.pack(">LLB",516,1,INmatrix))
        else:
            mysend(s,struct.pack(">LL",516,0))
        header = myrecv(s,8)
        cmd,length = struct.unpack(">LL",header)
        if cmd==517 and length==484:
            # This will generate a matrix of goals, from 0 to 10+
            # for both rows and columns. It does not include totals.
            for i in xrange(11):
                data = myrecv(s,44)
                matrix.append(list(struct.unpack(">LLLLLLLLLLL",data)))
    s.close()

    # Chunk operations info
    chunk_info = {}
    s = socket.socket()
    s.connect((masterhost,masterport))
    mysend(s,struct.pack(">LL",514,0))
    header = myrecv(s,8)
    cmd,length = struct.unpack(">LL",header)
    if cmd==515 and length==52:
        data = myrecv(s,length)
        loopstart,loopend,del_invalid,ndel_invalid,del_unused,ndel_unused,del_dclean,ndel_dclean,del_ogoal,ndel_ogoal,rep_ugoal,nrep_ugoal,rebalnce = struct.unpack(">LLLLLLLLLLLLL",data[:52])
        chunk_info = {
            'loop_start':                     loopstart,
            'loop_end':                       loopend,
            'invalid_deletions':              del_invalid,
            'invalid_deletions_out_of':       del_invalid+ndel_invalid,
            'unused_deletions':               del_unused,
            'unused_deletions_out_of':        del_unused+ndel_unused,
            'disk_clean_deletions':           del_dclean,
            'disk_clean_deletions_out_of':    del_dclean+ndel_dclean,
            'over_goal_deletions':            del_ogoal,
            'over_goal_deletions_out_of':     del_ogoal+ndel_ogoal,
            'replications_under_goal':        rep_ugoal,
            'replications_under_goal_out_of': rep_ugoal+nrep_ugoal,
            'replocations_rebalance':         rebalnce,
        }

    # Filesystem check info
    check_info = {}
    s = socket.socket()
    s.connect((masterhost,masterport))
    mysend(s,struct.pack(">LL",512,0))
    header = myrecv(s,8)
    cmd,length = struct.unpack(">LL",header)
    if cmd==513 and length>=36:
        data = myrecv(s,length)
        loopstart,loopend,files,ugfiles,mfiles,chunks,ugchunks,mchunks,msgbuffleng = struct.unpack(">LLLLLLLLL",data[:36])
        messages = data[36:]
        #messages = data[36:].replace("&","&amp;").replace(">","&gt;").replace("<","&lt;")
        check_info = {
            'check_loop_start_time': loopstart,
            'check_loop_end_time':   loopend,
            'files':                 files,
            'under_goal_files':      ugfiles,
            'missing_files':         mfiles,
            'chunks':                chunks,
            'under_goal_chunks':     ugchunks,
            'missing_chunks':        mchunks,
            'msgbuffleng':           msgbuffleng,
            'important_messages':    messages,
        }
    else:
        check_info = {
            'important_messages':    'No important messages',
        }

info_data()
sys.exit(0)
