#!/usr/bin/env python
##
## Analyze how your Ceph cluster performs (deep) scrubs
##
## Author:       Simon Leinen  <simon.leinen@switch.ch>
## Date created: 2015-04-07
##
## The input file should be generated using these commands:
##
##   ceph osd tree
##   foreach osd_host in $osd_hosts
##   do
##     ssh $osd_host zgrep scrub '/var/log/ceph/ceph-osd.*.{7,6,5,4,3,2,1}.gz'
##   done
##   ceph pg dump

import os
import re
from datetime import datetime
from datetime import timedelta

LOG = '20150407-scrub-logs.txt'

class ParseError(Exception):
    pass

class PG:

    """Ceph placement group"""

    def __init__(
            self,
            pgid,
            bytes=0,
            up=[],
            acting=[],
            hosts=[]):
        self.pgid = pgid
        self.bytes = bytes
        self.up = up
        self.acting = acting
        self.hosts = hosts

    def __str__(self):
        return "PG %6s (%6.2f GB) [%s] [%s]" \
            % (self.pgid,
               self.bytes * 1e-9,
               ",".join(self.hosts),
               ",".join(map(str,self.acting)))

class CephScrubLogAnalyzer:

    def __init__(
            self,
            log,
            min_time=None,
    ):
        self.log_file_name=log
        self.min_time=min_time

    def parse_osd_log_line(self, line):
        m = self.OSD_LOG_RE.match(line)
        if not m:
            return False
        osdno = m.group(1)
        tstamp = datetime.strptime(m.group(4), "%Y-%m-%d %H:%M:%S") \
                 +timedelta(microseconds=int(m.group(5)))
        if not self.min_time or self.min_time < tstamp:
            hex = m.group(6)
            pgid = m.group(7)
            self.osd_deep_scrub_log[tstamp] = pgid
            scrub_type = m.group(8)
            if scrub_type == 'scrub':
                self.shallow_count = self.shallow_count+1
            elif scrub_type == 'deep-scrub':
                self.deep_count = self.deep_count+1
            else:
                raise ParseError()
            self.scrub_count = self.scrub_count+1
        return True

    def parse_pg(self, line):

        def parse_osd_set(s):
            return map(int, s.split(","))

        def osd_host(osd):
            return self.osd_to_host[osd]

        m = self.PG_RE.match(line)
        if not m:
            return False
        pgid = m.group(1)
        bytes = int(m.group(2))
        status = m.group(3)
        up_set = parse_osd_set(m.group(5))
        up_primary = int(m.group(6))
        assert(up_set[0] == up_primary)
        acting_set = parse_osd_set(m.group(7))
        acting_primary = int(m.group(8))
        hosts = map(osd_host, acting_set)
        assert(acting_set[0] == acting_primary)
        self.pg[pgid] = PG(pgid, bytes=bytes, \
                           up=up_set, acting=acting_set,
                           hosts=hosts)
        return True

    def parse_osd_tree_host(self, line):
        m = self.OSD_TREE_HOST_RE.match(line)
        if not m:
            return False
        self.current_host = m.group(3)
        return True

    def parse_osd_tree_osd(self, line):
        m = self.OSD_TREE_OSD_RE.match(line)
        if not m:
            return False
        osdno = int(m.group(1))
        self.osd_to_host[osdno] = self.current_host
        return True

    def parse(self):
        self.OSD_LOG_RE = re.compile('^ */var/log/ceph/ceph-osd\.(\d+)\.log(\.\d+(\.gz)?)?:'
                                     +'(\d\d+-\d\d-\d\d \d\d:\d\d:\d\d)\.(\d+)\s+'
                                     +'([0-9a-f]+)\s+'
                                     +'0 log \[INF\] : (.*) (deep-scrub|scrub) ok}?$')
        self.OSD_TREE_HOST_RE = re.compile('^(-\d+)\t(\d+\.\d+)\t\thost (.*)$')
        self.OSD_TREE_OSD_RE = re.compile('^(\d+)\t(\d+\.\d+)\t\t\t'
                                          +'osd\.(\d+)\tup\t1\t$')
        self.PG_RE = re.compile('^([0-9a-f]+\.[0-9a-f]+)\t\d+\t\d+\t\d+\t'
                                +'\d+\t(\d+)\t\d+\t\d+\t(\S+)\t'
                                +'(\d\d\d+-\d\d-\d\d \d\d:\d\d:\d\d.\d*)\t'
                                +'\d+'+"'"+'\d+\t\d+:\d+\t\[([0-9,]+)\]\t'
                                +'(\d+)\t\[([0-9,]+)\]\t(\d+)\t'
                                +'\d+'+"'"+'\d+\t'
                                +'(\d\d\d+-\d\d-\d\d \d\d:\d\d:\d\d.\d*)\t'
                                +'\d+'+"'"+'\d+\t'
                                +'(\d\d\d+-\d\d-\d\d \d\d:\d\d:\d\d.\d*)$'
        )
        self.scrub_count, self.shallow_count, self.deep_count = 0, 0, 0
        self.osd_deep_scrub_log = dict()
        self.osd_to_host = dict()
        self.pg = dict()

        for line in open(self.log_file_name):
            if self.parse_osd_log_line(line):
                pass
            elif self.parse_pg(line):
                pass
            elif self.parse_osd_tree_host(line):
                pass
            elif self.parse_osd_tree_osd(line):
                pass
        print("Found %d scrubs, %d deep" % (self.scrub_count, self.deep_count))
        for time in sorted(self.osd_deep_scrub_log.keys()):
            pg = self.pg[self.osd_deep_scrub_log[time]]
            print("%s %s" % (time, pg))

ana = CephScrubLogAnalyzer(log=LOG,
                           min_time=datetime(2015,04,01))
ana.parse()
