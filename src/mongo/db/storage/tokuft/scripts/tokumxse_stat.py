#!/usr/bin/env python2

"""tokumxse_stat.py watches the serverStatus of a TokuMXse instance and periodically
prints when any variables changed, and by how much.  It is typically used for
monitoring a system.
"""

import collections
import logging
import re
import sys
import time
import optparse

import pymongo


def print_rec(stats, es, sleeptime, pfx):
    od = collections.OrderedDict(sorted(es.items()))
    
    for k, v in od.iteritems():
        if len(pfx) == 0:
            keyname = k
        else:
            keyname = '%s.%s' % (pfx, k)

        if type(v) == type({}):
            print_rec(stats, v, sleeptime, keyname)
            continue

        if stats.has_key(keyname):
            oldv = stats[keyname]
            if v != oldv:
                print keyname, "|", oldv, "|", v,
                d = v - oldv
                if sleeptime != 1:
                    try:
                        d_secs = d.total_seconds()
                    except AttributeError:
                        d_secs = d
                    if d_secs >= sleeptime:
                        e = d / sleeptime
                    else:
                        e = float(d) / sleeptime
                    print "|", d, "|", e
                else:
                    print "|", d
        stats[keyname] = v

def printit(stats, es, sleeptime):
    logging.info(time.strftime('%c'))
    print_rec(stats, es, sleeptime, '')
    print

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options] <host:port>\n\n" + __doc__)
    parser.add_option("-s", "--sleeptime", dest="sleeptime", default=10, type=int,
                      help="duration to sleep between reports [default: %default]", metavar="TIME")
    (opts, args) = parser.parse_args()
    if len(args) > 1:
        parser.error("too many arguments")
    if opts.sleeptime < 1:
        parser.error("invalid --sleeptime: %d" % opts.sleeptime)

    logging.basicConfig(level=logging.INFO)

    host = "localhost:27017"
    if len(args) == 1:
        host = args[0]

    try:
        logging.debug('connecting to %s...', host)
        client = pymongo.MongoClient(host)
        db = client['test']
    except:
        logging.exception('error connecting to %s', host)
        return 1

    logging.info('connected to %s', host)

    stats = {}
    try:
        while 1:
            try:
                es = db.command('serverStatus')
            except:
                logging.exception('error running serverStatus command')
                return 2

            try:
                printit(stats, es['tokuft'], int(opts.sleeptime))
            except:
                logging.exception('error printing info')
                return 3

            time.sleep(int(opts.sleeptime))

    except KeyboardInterrupt:
        logging.info('disconnecting')

    return 0

sys.exit(main())
