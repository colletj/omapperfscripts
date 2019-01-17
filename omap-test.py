#! /bin/python
#
# julien.collet@cern.ch 
# omap benchmarks, 2019

import rados, sys, time, threading

print("Running omap benchmarking tests")

if len(sys.argv) is 1:
    print "No arg provided\nUsage: ./omap-test.py <num omap pairs>\nExiting..."
    exit(-1); 

n = int(sys.argv[1]);

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
print "\nlibrados version: " + str(cluster.version())

cluster.connect()
print "Cluster ID: " + cluster.get_fsid()

if not (cluster.pool_exists('omap-test') == True):
   cluster.create_pool('omap-test')

ioctx = cluster.open_ioctx('omap-test')

print "\nBenchmarking synchronous omap ops"
print "------------------"

st = time.time()
for i in range(0,n):
    with rados.WriteOpCtx(ioctx) as wop:
        ioctx.set_omap(wop, (str(i),), ("val"+str(i),))
        ioctx.operate_write_op(wop, "test")

wr = time.time()

with rados.ReadOpCtx(ioctx) as rop:
    iter, r = ioctx.get_omap_vals(rop, "", "", n)
    ioctx.operate_read_op(rop, "test")

rd = time.time()

print "set (sync) " + str(n) + " omap pairs in " + str(wr - st) + "s"
print "got (sync) " + str(n) + " omap pairs in " + str(rd - wr) + "s"

print "\nBenchmarking asynchronous omap ops"
print "------------------"

st = time.time()
for i in range(0,n):
    lock = threading.Condition()
    count = [0]
    def cb(blah):
        with lock:
            count[0] += 1
            lock.notify()
        return 0
    
    with rados.WriteOpCtx(ioctx) as wop:
            ioctx.set_omap(wop, (str(i),), ("aio"+str(i),))
            comp = ioctx.operate_aio_write_op(wop, "test", cb, cb)
            comp.wait_for_complete()
            comp.wait_for_safe()
            with lock:
                while count[0] < 2:
                    lock.wait()
            comp.get_return_value()

wr = time.time()

count = [0]
with rados.ReadOpCtx(ioctx) as rop:
    iter, r = ioctx.get_omap_vals(rop, "", "", n)
    ioctx.operate_aio_read_op(rop, "test", cb, cb)
    comp.wait_for_complete()
    comp.wait_for_safe()
    with lock:
        while count[0] < 2:
            lock.wait()
    comp.get_return_value()
   
rd = time.time()

print "set (aio) " + str(n) + " omap pairs in " + str(wr - st) + "s"
print "got (aio) " + str(n) + " omap pairs in " + str(rd - wr) + "s"

ioctx.close()
cluster.delete_pool('omap-test')



