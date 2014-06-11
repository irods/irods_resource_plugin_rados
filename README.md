irods_resource_plugin_rados
===========================

Cacheless Ceph/rados resource plugin for iRODS

=============== TODOS: =============
- [ ] rename file-example to irados
- [X] singleton ceph context initialization
- [ ] Cleanup of io_ctx instances on plugin close? -> i.e. after every read write operation?



=============== Notes: =============
To install this example plugin:
 $ make
 $ cp libexamplefilesystem.so /var/lib/irods/plugins/resources
 $ chown irods:irods /var/lib/irods/plugins/resources/libexamplefilesystem.so


To create a new resource:

iadmin mkresc irados irados charon:/Vault/irados/



# access files directly from rados
rados --pool=irods ls



installation of librados:

http://ceph.com/debian/pool/main/c/ceph/librados2_0.72.2-1precise_amd64.deb


dpkg -i librados-dev_0.72.2-1raring_amd64.deb librados2_0.72.2-1raring_amd64.deb


ceph auth get-or-create client.irods osd 'allow rw' mon 'allow r' > /etc/ceph/keyring.irods
scp /etc/ceph/keyring.irods archive-ceph-03:/etc/ceph/


[client.irods]
        key = AQCubxhTGFE2BhAA6iATSh3WqWBDIeOgnicBiA==

        
 usage:
 
 http://ceph.com/docs/master/rados/api/librados-intro/
 
 
 
 
 it seems that ceph / rados also has to be installed on the icat?