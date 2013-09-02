#!/usr/bin/env python2
'''
A Python library for gathering information from MooseFS
'''
import socket
import struct
import time
import traceback
import sys

class MooseFS():
    """
    Class for querying MooseFS
    """

    def __init__(self, masterhost='mfsmaster', masterport=9421 ):
        self.masterhost = masterhost
        self.masterport = masterport
        self.masterversion = self.check_master_version()

    def mysend(self, socket, msg):
        totalsent = 0
        while totalsent < len(msg):
            sent = socket.send(msg[totalsent:])
            if sent == 0:
                raise RuntimeError("socket connection broken")
            totalsent = totalsent + sent

    def myrecv(self, socket, leng):
        msg = ''
        while len(msg) < leng:
            chunk = socket.recv(leng-len(msg))
            if chunk == '':
                raise RuntimeError("socket connection broken")
            msg = msg + chunk
        return msg

    def check_master_version(self):
        masterversion = (0, 0, 0)
        s = socket.socket()
        s.connect((self.masterhost, self.masterport))
        self.mysend(s, struct.pack(">LL", 510, 0))
        header = self.myrecv(s, 8)
        cmd, length = struct.unpack(">LL", header)
        data = self.myrecv(s, length)
        if cmd == 511:
            if length == 52:
                masterversion = (1, 4, 0)
            elif length == 60:
                masterversion = (1, 5, 0)
            elif length == 68 or length == 76:
                masterversion = struct.unpack(">HBB", data[:4])
        return masterversion

    def mfs_info(self, INmatrix=0):
        # For INmatrix, 0 means all, 1 means regular
        info = {}
        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 510, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 511 and length == 52:
                data = self.myrecv(s, length)
                total, avail, trspace, trfiles, respace, refiles, nodes, chunks, tdcopies = struct.unpack(">QQQLQLLLL", data)
                info = {
                    'total_space':          total,
                    'avail_space':          avail,
                    'trash_space':          trspace,
                    'trash_files':          trfiles,
                    'reserved_space':       respace,
                    'reserved_files':       refiles,
                    'all_fs_objects':       nodes,
                    'chunks':               chunks,
                    'regular_chunk_copies': tdcopies,
                }
            elif cmd == 511 and length == 60:
                data = self.myrecv(s, length)
                total, avail, trspace, trfiles, respace, refiles, nodes, dirs, files, chunks, tdcopies = struct.unpack(">QQQLQLLLLLL", data)
                info = {
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
                    'regular_chunk_copies': tdcopies,
                }
            elif cmd == 511 and length == 68:
                data = self.myrecv(s, length)
                v1, v2, v3, total, avail, trspace, trfiles, respace, refiles, nodes, dirs, files, chunks, allcopies, tdcopies = struct.unpack(">HBBQQQLQLLLLLLL", data)
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
            elif cmd == 511 and length == 76:
                data = self.myrecv(s, length)
                v1, v2, v3, memusage, total, avail, trspace, trfiles, respace, refiles, nodes, dirs, files, chunks, allcopies, tdcopies = struct.unpack(">HBBQQQQLQLLLLLLL", data)
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
                    'memusage':             memusage,
                }
            else:
                info = {
                    'error': 'unrecognized answer from MFSmaster',
                }
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        # All chunks state matrix
        matrix = []
        if self.masterversion >= (1, 5, 13):
            try:
                s = socket.socket()
                s.connect((self.masterhost, self.masterport))
                if self.masterversion >= (1, 6, 10):
                    self.mysend(s, struct.pack(">LLB", 516, 1, INmatrix))
                else:
                    self.mysend(s, struct.pack(">LL", 516, 0))
                header = self.myrecv(s, 8)
                cmd, length = struct.unpack(">LL", header)
                if cmd == 517 and length == 484:
                    # This will generate a matrix of goals, from 0 to 10+
                    # for both rows and columns. It does not include totals.
                    for i in xrange(11):
                        data = self.myrecv(s, 44)
                        matrix.append(list(struct.unpack(">LLLLLLLLLLL", data)))
                s.close()
            except Exception:
                traceback.print_exc(file=sys.stdout)

        # Chunk operations info
        chunk_info = {}
        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 514, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 515 and length == 52 or length == 76:
                data = self.myrecv(s, length)
                loopstart, loopend, del_invalid, ndel_invalid, del_unused, ndel_unused, del_dclean, ndel_dclean, del_ogoal, ndel_ogoal, rep_ugoal, nrep_ugoal, rebalnce = struct.unpack(">LLLLLLLLLLLLL", data[:52])
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
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        # Filesystem check info
        check_info = {}
        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 512, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 513 and length >= 36:
                data = self.myrecv(s, length)
                loopstart, loopend, files, ugfiles, mfiles, chunks, ugchunks, mchunks, msgbuffleng = struct.unpack(">LLLLLLLLL", data[:36])
                messages = ''
                truncated = ''
                if loopstart > 0:
                    if msgbuffleng > 0:
                        if msgbuffleng == 100000:
                            truncated = 'first 100k'
                        else:
                            truncated = 'no'
                        messages = data[36:]
                else:
                    messages = 'no data'
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
                    'truncated':             truncated,
                }
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        ret = {
            'info': info,
            'matrix': matrix,
            'chunk_info': chunk_info,
            'check_info': check_info,
        }
        return ret

    def mfs_servers(self):
        servers = []
        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 500, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 501 and self.masterversion >= (1, 5, 13) and (length % 54) == 0:
                data = self.myrecv(s, length)
                n = length/54
                for i in xrange(n):
                    d = data[i*54:(i+1)*54]
                    v1, v2, v3, ip1, ip2, ip3, ip4, port, used, total, chunks, tdused, tdtotal, tdchunks, errcnt = struct.unpack(">HBBBBBBHQQLQQLL", d)
                    host = ''
                    try:
                        host = (socket.gethostbyaddr("%u.%u.%u.%u" % (ip1, ip2, ip3, ip4)))[0]
                    except Exception:
                        host = "(unresolved)"
                    ip = '.'.join([str(ip1), str(ip2), str(ip3), str(ip4)])
                    ver = '.'.join([str(v1), str(v2), str(v3)])
                    percent_used = ''
                    if (total>0):
                        percent_used = (used*100.0)/total
                    else:
                        percent_used = '-'
                    tdpercent_used = ''
                    if (tdtotal>0):
                        tdpercent_used = (tdused*100.0)/tdtotal
                    else:
                        tdpercent_used = ''
                    servers.append({
                        'host':           host,
                        'ip':             ip,
                        'version':        ver,
                        'port':           port,
                        'used':           used,
                        'total':          total,
                        'chunks':         chunks,
                        'percent_used':   percent_used,
                        'tdused':         tdused,
                        'tdtotal':        tdtotal,
                        'tdchucnks':      tdchunks,
                        'tdpercent_used': tdpercent_used,
                        'errcount':       errcnt,
                    })
            elif cmd == 501 and self.masterversion < (1, 5, 13) and (length % 50) == 0:
                data = self.myrecv(s, length)
                n = length/50
                for i in xrange(n):
                    d = data[i*50:(i+1)*50]
                    ip1, ip2, ip3, ip4, port, used, total, chunks, tdused, tdtotal, tdchunks, errcnt = struct.unpack(">BBBBHQQLQQLL", d)
                    try:
                        host = (socket.gethostbyaddr("%u.%u.%u.%u" % (ip1, ip2, ip3, ip4)))[0]
                    except Exception:
                        host = "(unresolved)"
                    ip = '.'.join([str(ip1), str(ip2), str(ip3), str(ip4)])
                    percent_used = ''
                    if (total>0):
                        percent_used = (used*100.0)/total
                    else:
                        percent_used = '-'
                    tdpercent_used = ''
                    if (tdtotal>0):
                        tdpercent_used = (tdused*100.0)/tdtotal
                    else:
                        tdpercent_used = ''
                    servers.append({
                        'host':           host,
                        'ip':             ip,
                        'port':           port,
                        'used':           used,
                        'total':          total,
                        'chunks':         chunks,
                        'percent_used':   percent_used,
                        'tdused':         tdused,
                        'tdtotal':        tdtotal,
                        'tdchucnks':      tdchunks,
                        'tdpercent_used': tdpercent_used,
                        'errcount':       errcnt,
                    })
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        # Metadata backup loggers
        mbloggers = []
        if self.masterversion >= (1, 6, 5):
            try:
                s = socket.socket()
                s.connect((self.masterhost, self.masterport))
                self.mysend(s, struct.pack(">LL", 522, 0))
                header = self.myrecv(s, 8)
                cmd, length = struct.unpack(">LL", header)
                if cmd == 523 and (length % 8) == 0:
                    data = self.myrecv(s, length)
                    n = length/8
                    for i in xrange(n):
                        d = data[i*8:(i+1)*8]
                        v1, v2, v3, ip1, ip2, ip3, ip4 = struct.unpack(">HBBBBBB", d)
                        ip = '.'.join([str(ip1), str(ip2), str(ip3), str(ip4)])
                        ver = '.'.join([str(v1), str(v2), str(v3)])
                        try:
                            host = (socket.gethostbyaddr("%u.%u.%u.%u" % (ip1, ip2, ip3, ip4)))[0]
                        except Exception:
                            host = "(unresolved)"
                        mbloggers.append((host, ip, ver))
                s.close()
            except Exception:
                traceback.print_exc(file=sys.stdout)
        ret = {
            'servers':                 servers,
            'metadata_backup_loggers': mbloggers,
        }
        return ret

    def mfs_disks(self, HDtime='max', HDperiod='min'):
        # HDtime can be avg or max
        # HDperiod can be min, hour or day
        try:
            # get cs list
            hostlist = []
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 500, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 501 and self.masterversion >= (1, 5, 13) and (length % 54) == 0:
                data = self.myrecv(s, length)
                n = length / 54
                servers = []
                for i in xrange(n):
                    d = data[i*54:(i+1)*54]
                    v1, v2, v3, ip1, ip2, ip3, ip4, port, used, total, chunks, tdused, tdtotal, tdchunks, errcnt = struct.unpack(">HBBBBBBHQQLQQLL", d)
                    hostlist.append((v1, v2, v3, ip1, ip2, ip3, ip4, port))
            elif cmd == 501 and self.masterversion < (1, 5, 13) and (length % 50) == 0:
                data = self.myrecv(s, length)
                n = length/50
                servers = []
                for i in xrange(n):
                    d = data[i*50:(i+1)*50]
                    ip1, ip2, ip3, ip4, port, used, total, chunks, tdused, tdtotal, tdchunks, errcnt = struct.unpack(">BBBBHQQLQQLL", d)
                    hostlist.append((1, 5, 0, ip1, ip2, ip3, ip4, port))
            s.close()

            # get hdd lists one by one
            hdd = []
            for v1, v2, v3, ip1, ip2, ip3, ip4, port in hostlist:
                hostip = "%u.%u.%u.%u" % (ip1, ip2, ip3, ip4)
                try:
                    hoststr = (socket.gethostbyaddr(hostip))[0]
                except Exception:
                    hoststr = "(unresolved)"
                if port > 0:
                    if (v1, v2, v3) <= (1, 6, 8):
                        s = socket.socket()
                        s.connect((hostip, port))
                        self.mysend(s, struct.pack(">LL", 502, 0))
                        header = self.myrecv(s, 8)
                        cmd, length = struct.unpack(">LL", header)
                        if cmd == 503:
                            data = self.myrecv(s, length)
                            while length > 0:
                                plen = ord(data[0])
                                host_path = "%s:%u:%s" % (hoststr, port, data[1:plen+1])
                                ip_path = "%s:%u:%s" % (hostip, port, data[1:plen+1])
                                flags, errchunkid, errtime, used, total, chunkscnt = struct.unpack(">BQLQQL", data[plen+1:plen+34])
                                length -= plen+34
                                data = data[plen+34:]
                                hdd.append((ip_path, host_path, flags, errchunkid, errtime, used, total, chunkscnt, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
                        s.close()
                    else:
                        s = socket.socket()
                        s.connect((hostip, port))
                        self.mysend(s, struct.pack(">LL", 600, 0))
                        header = self.myrecv(s, 8)
                        cmd, length = struct.unpack(">LL", header)
                        if cmd == 601:
                            data = self.myrecv(s, length)
                            while length > 0:
                                entrysize = struct.unpack(">H", data[:2])[0]
                                entry = data[2:2+entrysize]
                                data = data[2+entrysize:]
                                length -= 2 + entrysize

                                plen = ord(entry[0])
                                host_path = "%s:%u:%s" % (hoststr, port, entry[1:plen+1])
                                ip_path = "%s:%u:%s" % (hostip, port, entry[1:plen+1])
                                flags, errchunkid, errtime, used, total, chunkscnt = struct.unpack(">BQLQQL", entry[plen+1:plen+34])
                                rbytes, wbytes, usecreadsum, usecwritesum, usecfsyncsum, rops, wops, fsyncops, usecreadmax, usecwritemax, usecfsyncmax = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                                if entrysize == plen + 34 + 144:
                                    if HDperiod == 'min':
                                        rbytes, wbytes, usecreadsum, usecwritesum, rops, wops, usecreadmax, usecwritemax = struct.unpack(">QQQQLLLL", entry[plen+34:plen+34+48])
                                    elif HDperiod == 'hour':
                                        rbytes, wbytes, usecreadsum, usecwritesum, rops, wops, usecreadmax, usecwritemax = struct.unpack(">QQQQLLLL", entry[plen+34+48:plen+34+96])
                                    elif HDperiod == 'day':
                                        rbytes, wbytes, usecreadsum, usecwritesum, rops, wops, usecreadmax, usecwritemax = struct.unpack(">QQQQLLLL", entry[plen+34+96:plen+34+144])
                                elif entrysize == plen + 34 + 192:
                                    if HDperiod == 'min':
                                        rbytes, wbytes, usecreadsum, usecwritesum, usecfsyncsum, rops, wops, fsyncops, usecreadmax, usecwritemax, usecfsyncmax = struct.unpack(">QQQQQLLLLLL", entry[plen+34:plen+34+64])
                                    elif HDperiod == 'hour':
                                        rbytes, wbytes, usecreadsum, usecwritesum, usecfsyncsum, rops, wops, fsyncops, usecreadmax, usecwritemax, usecfsyncmax = struct.unpack(">QQQQQLLLLLL", entry[plen+34+64:plen+34+128])
                                    elif HDperiod == 'day':
                                        rbytes, wbytes, usecreadsum, usecwritesum, usecfsyncsum, rops, wops, fsyncops, usecreadmax, usecwritemax, usecfsyncmax = struct.unpack(">QQQQQLLLLLL", entry[plen+34+128:plen+34+192])
                                if usecreadsum > 0:
                                    rbw = rbytes * 1000000 / usecreadsum
                                else:
                                    rbw = 0
                                if usecwritesum + usecfsyncsum > 0:
                                    wbw = wbytes *1000000 / (usecwritesum + usecfsyncsum)
                                else:
                                    wbw = 0
                                if HDtime == 'avg':
                                    if rops > 0:
                                        rtime = usecreadsum/rops
                                    else:
                                        rtime = 0
                                    if wops > 0:
                                        wtime = usecwritesum/wops
                                    else:
                                        wtime = 0
                                    if fsyncops > 0:
                                        fsynctime = usecfsyncsum/fsyncops
                                    else:
                                        fsynctime = 0
                                else:
                                    rtime = usecreadmax
                                    wtime = usecwritemax
                                    fsynctime = usecfsyncmax
                                if flags == 1:
                                    if self.masterversion >= (1, 6, 10):
                                        status = 'marked for removal'
                                    else:
                                        status = 'to be empty'
                                elif flags == 2:
                                    status = 'damaged'
                                elif flags == 3:
                                    if self.masterversion >= (1, 6, 10):
                                        status = 'damaged, marked for removal'
                                    else:
                                        status = 'damaged, to be empty'
                                else:
                                    status = 'ok'
                                if errtime == 0 and errchunkid == 0:
                                    lerror = 'no errors'
                                else:
                                    lerror = time.localtime(errtime)
                                if rops > 0:
                                    rbsize = rbytes / rops
                                else:
                                    rbsize = 0
                                if wops > 0:
                                    wbsize = wbytes / wops
                                else:
                                    wbsize = 0
                                if (total >0 ):
                                    percent_used = (used * 100.0) / total
                                else:
                                    percent_used = '-'
                                hdd.append({
                                    'ip_path':      ip_path,
                                    'host_path':    host_path,
                                    'flags':        flags,
                                    'errchunkid':   errchunkid,
                                    'errtime':      errtime,
                                    'used':         used,
                                    'total':        total,
                                    'chunkscount':  chunkscnt,
                                    'rbw':          rbw,
                                    'wbw':          wbw,
                                    'rtime':        rtime,
                                    'wtime':        wtime,
                                    'fsynctime':    fsynctime,
                                    'read_ops':     rops,
                                    'write_ops':    wops,
                                    'fsyncops':     fsyncops,
                                    'read_bytes':   rbytes,
                                    'write_bytes':  wbytes,
                                    'usecreadsum':  usecreadsum,
                                    'usecwritesum': usecwritesum,
                                    'status':       status,
                                    'lerror':       lerror,
                                    'rbsize':       rbsize,
                                    'wbsize':       wbsize,
                                    'percent_used': percent_used,
                                })
                        s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        return hdd

    def mfs_exports(self):
        servers = []
        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 520, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 521 and self.masterversion >= (1, 5, 14):
                data = self.myrecv(s, length)
                pos = 0
                while pos < length:
                    fip1, fip2, fip3, fip4, tip1, tip2, tip3, tip4, pleng = struct.unpack(">BBBBBBBBL", data[pos:pos+12])
                    ipfrom = "%d.%d.%d.%d" % (fip1, fip2, fip3, fip4)
                    ipto = "%d.%d.%d.%d" % (tip1,  tip2, tip3, tip4)
                    pos += 12
                    path = data[pos:pos+pleng]
                    pos += pleng
                    if self.masterversion >= (1, 6, 1):
                        v1, v2, v3, exportflags, sesflags, rootuid, rootgid, mapalluid, mapallgid = struct.unpack(">HBBBBLLLL", data[pos:pos+22])
                        pos += 22
                        if sesflags & 16 == 0:
                            mapalluid = None
                            mapallgid = None
                    else:
                        v1, v2, v3, exportflags, sesflags, rootuid, rootgid = struct.unpack(">HBBBBLL", data[pos:pos+14])
                        mapalluid = 0
                        mapallgid = 0
                        pos += 14
                    ver = "%d.%d.%d" % (v1, v2, v3)
                    if path == '.':
                        meta = 1
                    else:
                        meta = 0
                    servers.append( {
                        'ip_range_from': ipfrom,
                        'ip_range_to':   ipto,
                        'path':          path,
                        'meta':          meta,
                        'version':       ver,
                        'export_flags':  exportflags,
                        'ses_flags':     sesflags,
                        'root_uid':      rootuid,
                        'root_gid':      rootgid,
                        'all_users_uid': mapalluid,
                        'all_users_gid': mapallgid,
                    } )
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        return servers

    def mfs_mountl(self):
        # This section appeared in the original mfs.cgi,
        # but doesn't actually show up in the browser.
        # An old version of section MS, perhaps?
        # I have left it here for historical reasons.
        servers = []

        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 508, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 509 and self.masterversion <= (1, 5, 13) and (length % 136) == 0:
                data = self.myrecv(s, length)
                n = length/136
                for i in xrange(n):
                    d = data[i*136:(i+1)*136]
                    addrdata = d[0:8]
                    stats_c = []
                    stats_l = []
                    ip1, ip2, ip3, ip4, spare, v1, v2, v3 = struct.unpack(">BBBBBBBB", addrdata)
                    ipnum = "%d.%d.%d.%d" % (ip1, ip2, ip3, ip4)
                    if v1 == 0 and v2 == 0:
                        if v3 == 2:
                            ver = "1.3.x"
                        elif v3 == 3:
                            ver = "1.4.x"
                        else:
                            ver = "unknown"
                    else:
                        ver = "%d.%d.%d" % (v1, v2, v3)
                    for i in xrange(16):
                        stats_c.append(struct.unpack(">L", d[i*4+8:i*4+12]))
                        stats_l.append(struct.unpack(">L", d[i*4+72:i*4+76]))
                    try:
                        host = (socket.gethostbyaddr(ipnum))[0]
                    except Exception:
                        host = "(unresolved)"
                    servers.append((host, ipnum, ver, stats_c, stats_l))
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        return servers

    def mfs_mounts(self):
        servers = []

        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            if self.masterversion >= (1, 6, 26):
                self.mysend(s, struct.pack(">LLB", 508, 1, 1))
            else:
                self.mysend(s, struct.pack(">LL", 508, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 509 and self.masterversion >= (1, 5, 14):
                data = self.myrecv(s, length)
                if self.masterversion < (1, 6, 21):
                    statscnt = 16
                    pos = 0
                elif self.masterversion == (1, 6, 21):
                    statscnt = 21
                    pos = 0
                else:
                    statscnt = struct.unpack(">H", data[0:2])[0]
                    pos = 2
                while pos < length:
                    sessionid, ip1, ip2, ip3, ip4, v1, v2, v3, ileng = struct.unpack(">LBBBBHBBL", data[pos:pos+16])
                    ipnum = "%d.%d.%d.%d" % (ip1, ip2, ip3, ip4)
                    ver = "%d.%d.%d" % (v1, v2, v3)
                    pos += 16
                    info = data[pos:pos+ileng]
                    pos += ileng
                    pleng = struct.unpack(">L", data[pos:pos+4])[0]
                    pos += 4
                    path = data[pos:pos+pleng]
                    pos += pleng
                    if self.masterversion >= (1, 6, 26):
                        sesflags, rootuid, rootgid, mapalluid, mapallgid, mingoal, maxgoal, mintrashtime, maxtrashtime = struct.unpack(">BLLLLBBLL", data[pos:pos+27])
                        pos += 27
                        if mingoal <= 1 and maxgoal >= 9:
                            mingoal = None
                            maxgoal = None
                        if mintrashtime == 0 and maxtrashtime == 0xFFFFFFFF:
                            mintrashtime = None
                            maxtrashtime = None
                    elif self.masterversion >= (1, 6, 1):
                        sesflags, rootuid, rootgid, mapalluid, mapallgid = struct.unpack(">BLLLL", data[pos:pos+17])
                        mingoal = None
                        maxgoal = None
                        mintrashtime = None
                        maxtrashtime = None
                        pos += 17

                    else:
                        sesflags, rootuid, rootgid = struct.unpack(">BLL", data[pos:pos+9])
                        mapalluid = 0
                        mapallgid = 0
                        mingoal = None
                        maxgoal = None
                        mintrashtime = None
                        maxtrashtime = None
                        pos += 9
                    pos += 8 * statscnt
                    if path == '.':
                        meta = 1
                    else:
                        meta = 0
                    try:
                        host = (socket.gethostbyaddr(ipnum))[0]
                    except Exception:
                        host = "(unresolved)"
                    servers.append( {
                        'sessionid':     sessionid,
                        'host':          host,
                        'ip':            ipnum,
                        'mount':         info,
                        'version':       ver,
                        'meta':          meta,
                        'moose_path':    path,
                        'ses_flags':     sesflags,
                        'root_uid':      rootuid,
                        'root_gid':      rootgid,
                        'all_users_uid': mapalluid,
                        'all_users_gid': mapallgid,
                        'mingoal':       mingoal,
                        'maxgoal':       maxgoal,
                        'mintrashtime':  mintrashtime,
                        'maxtrashtime':  maxtrashtime,
                    } )
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        return servers

    def mfs_operations(self):
        servers = []

        try:
            s = socket.socket()
            s.connect((self.masterhost, self.masterport))
            self.mysend(s, struct.pack(">LL", 508, 0))
            header = self.myrecv(s, 8)
            cmd, length = struct.unpack(">LL", header)
            if cmd == 509 and self.masterversion >= (1, 5, 14):
                data = self.myrecv(s, length)
                pos = 0
                if self.masterversion < (1, 6, 21):
                    statscnt = 16
                    pos = 0
                elif self.masterversion == (1, 6, 21):
                    statscnt = 21
                    pos = 0
                else:
                    statscnt = struct.unpack(">H", data[0:2])[0]
                    pos = 2
                while pos < length:
                    sessionid, ip1, ip2, ip3, ip4, v1, v2, v3, ileng = struct.unpack(">LBBBBHBBL", data[pos:pos+16])
                    ipnum = "%d.%d.%d.%d" % (ip1, ip2, ip3, ip4)
                    ver = "%d.%d.%d" % (v1, v2, v3)
                    pos += 16
                    info = data[pos:pos+ileng]
                    pos += ileng
                    pleng = struct.unpack(">L", data[pos:pos+4])[0]
                    pos += 4
                    path = data[pos:pos+pleng]
                    pos += pleng
                    if self.masterversion >= (1, 6, 0):
                        pos += 17
                    else:
                        pos += 9
                    if statscnt < 16:
                        stats_c = struct.unpack(">"+"L"*statscnt, data[pos:pos+4*statscnt])+(0,)*(16-statscnt)
                        pos += statscnt * 4
                        stats_l = struct.unpack(">"+"L"*statscnt, data[pos:pos+4*statscnt])+(0,)*(16-statscnt)
                        pos += statscnt * 4
                    else:
                        stats_c = struct.unpack(">LLLLLLLLLLLLLLLL", data[pos:pos+64])
                        pos += statscnt * 4
                        stats_l = struct.unpack(">LLLLLLLLLLLLLLLL", data[pos:pos+64])
                        pos += statscnt * 4
                    try:
                        host = (socket.gethostbyaddr(ipnum))[0]
                    except Exception:
                        host = "(unresolved)"
                    if path != '.':
                        servers.append( {
                            'host':           host,
                            'ip':             ipnum,
                            'info':           info,
                            'ver':            ver,
                            'sessionid':      sessionid,
                            'stats_current':  {
                                'statfs':   stats_c[0],
                                'getattr':  stats_c[1],
                                'setattr':  stats_c[2],
                                'lookup':   stats_c[3],
                                'mkdir':    stats_c[4],
                                'rmdir':    stats_c[5],
                                'symlink':  stats_c[6],
                                'readlink': stats_c[7],
                                'mknod':    stats_c[8],
                                'unlink':   stats_c[9],
                                'rename':   stats_c[10],
                                'link':     stats_c[11],
                                'readdir':  stats_c[12],
                                'open':     stats_c[13],
                                'read':     stats_c[14],
                                'write':    stats_c[15],
                            },
                            'stats_lasthour': {
                                'statfs':   stats_l[0],
                                'getattr':  stats_l[1],
                                'setattr':  stats_l[2],
                                'lookup':   stats_l[3],
                                'mkdir':    stats_l[4],
                                'rmdir':    stats_l[5],
                                'symlink':  stats_l[6],
                                'readlink': stats_l[7],
                                'mknod':    stats_l[8],
                                'unlink':   stats_l[9],
                                'rename':   stats_l[10],
                                'link':     stats_l[11],
                                'readdir':  stats_l[12],
                                'open':     stats_l[13],
                                'read':     stats_l[14],
                                'write':    stats_l[15],
                            },
                        } )
            s.close()
        except Exception:
            traceback.print_exc(file=sys.stdout)

        return servers
