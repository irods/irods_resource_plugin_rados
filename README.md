# irods_resource_plugin_rados

Cacheless Ceph/rados resource plugin for iRODS


## Installation 

Currently, there are no prebuilt packages, but Ubuntu 12.04 and CentOS6.5 have been successfully tested.

Prequesites for Ubuntu:

```
apt-get install uuid-dev libssl-dev build-essential

wget http://ceph.com/debian/pool/main/c/ceph/librados2_0.80-1precise_amd64.deb
wget http://ceph.com/debian/pool/main/c/ceph/librados-dev_0.80-1precise_amd64.deb

sudo dpkg -i librados*.deb
sudo apt-get install -f
```

Then checkout, build and install the plugin.

```
git clone https://github.com/meatz/irods_resource_plugin_rados.git
cd irods_resource_plugin_rados
make
make install
```

The plugin needs to be present on both, the iCAT server and the resource server.

## Setup

Create an irods pool on ceph, i.e.

```
ceph osd pool create irods
ceph auth get-or-create client.irods osd 'allow rw pool=irods' mon 'allow r' > /etc/ceph/client.irods.keyring
```

Copy the key from the newly created keyring and create the ceph config files on the resource server:

/etc/irods/irados.config
```
[global]
mon host = hostname_of_any_ceph_monitor
[client.irods]
        keyring = /etc/irods/irados.keyring
```

and /etc/irods/irados.keyring
```
[client.irods]
        key = ACME/SECRET/KEY==
```

If no context like :/tmp/ is provided, the plugin does not work correctly. Nevertheless, the context is not used at all.
```
iadmin mkresc irados irados irods-rs:/tmp/
```

Then upload files with:

```
iput -R irados files/
```
