#! /bin/python
#
# julien.collet@cern.ch 
# omap benchmarks, 2019

import rados, sys, time, threading

print("Running omap benchmarking tests");

cluster = rados.Rados(conffile='/etc/ceph/ceph.conf');
print "\nlibrados version: " + str(cluster.version())
print "Will attempt to connect to: " + str(cluster.conf_get('mon initial members'))

cluster.connect()
print "\nCluster ID: " + cluster.get_fsid()

print "\n\nCluster Statistics"
print "=================="
cluster_stats = cluster.get_cluster_stats()

for key, value in cluster_stats.iteritems():
    print key, value

print "\n\nPool Operations"
print "==============="

print "\nAvailable Pools"
print "----------------"
pools = cluster.list_pools()

for pool in pools:
        print pool

print "\nCreate 'omap-test' Pool"
print "------------------"
if not (cluster.pool_exists('omap-test') == True):
   cluster.create_pool('omap-test')

print "\nPool named 'omap-test' exists: " + str(cluster.pool_exists('omap-test'))
print "\nVerify 'omap-test' Pool Exists"
print "-------------------------"
pools = cluster.list_pools()

for pool in pools:
    print pool

ioctx = cluster.open_ioctx('omap-test');

print "\nListing objects in the pool"
print "------------------"


n=1000


print "\nBenchmarking synchronous omap ops"
print "------------------"

st = time.time()
with rados.WriteOpCtx(ioctx) as wop:
    for i in range(0,n):
        ioctx.set_omap(wop, (str(i),), ("val"+str(i),));
        ioctx.operate_write_op(wop, "test");


wr = time.time();

with rados.ReadOpCtx(ioctx) as rop:
    iter, r = ioctx.get_omap_vals(rop, "", "", n);
    ioctx.operate_read_op(rop, "test");

rd = time.time();

print "set (sync) " + str(n) + " omap pairs in " + str(wr - st) + "s"
print "got (sync) " + str(n) + " omap pairs in " + str(rd - wr) + "s"



#print "\nBenchmarking asynchronous omap ops"
#print "------------------"
#
#
#lock = threading.Condition();
#count = [0];
#def cb(blah):
#    with lock:
#        count[0] += 1;
#        lock.notify();
#    return 0;
#
#st = time.time()
#with rados.WriteOpCtx(ioctx) as wop:
#    for i in range(0,n):
#        ioctx.set_omap(wop, (str(i),), ("val"+str(i),));
#        comp = ioctx.operate_aio_write_op(wop, "test", cb, cb);
#        comp.wait_for_complete();
#        comp.wait_for_safe();
#        with lock:
#            while count[0] < 10:
#                lock.wait();
#        comp.get_return_value()
#        
#
#wr = time.time();
#
##with rados.ReadOpCtx(ioctx) as rop:
##    iter, r = ioctx.get_omap_vals(rop, "", "", n);
##    ioctx.operate_read_op(rop, "test");
#
#rd = time.time();
#
#print "set (aio) " + str(n) + " omap pairs in " + str(wr - st) + "s"
#print "got (aio) " + str(n) + " omap pairs in " + str(rd - wr) + "s"





print "\nClosing the connection."
ioctx.close();

print "\nDelete 'omap=test' Pool"
print "------------------"
cluster.delete_pool('omap-test')
print "\nPool named 'omap-test' exists: " + str(cluster.pool_exists('omap-test'))



