# irods_resource_plugin_rados

Cacheless Ceph/rados resource plugin for iRODS

First presentation at iRODS user group meeting 2014 / Boston, MA - http://www.slideshare.net/mgrawinkel/development-of-the-irods-rados-plugin

## Introduction

This iRODS plugin implements a direct access to Ceph/rados in the most efficient manner.
Files in the iRODS namespace are mapped to objects in the rados key-blob store.
In contrast to other plugins, the irados resource plugin does not need to cache or stage files, but gives you direct and parallel access to data.
Internally, the plugin maps the POSIX like open, read, write, seek, unlink, stat, and close calls to the librados client's operations.
To fully use the inherent rados cluster parallelity, irods files are split to multiple 4mb files and uploads of large files open multiple parallel transfer threads.

The plugin assumes that file's ACLs as well as its namespace and metadata is fully managed by iRODS.
Rados simply stores the bytes of the file.

For every new file, a unique uuid is generated as the primary access key in rados. This uuid is set as the physical path of the file in the iRODS icat.
Files are spread to 4mb blobs that are named by an incrementing identifier suffix.
The first object is called by its uuid and contains extended attributes to store the actual size of the file and the number of blobs that make it up.
All following files are named as uuid-1, uuid-2, ...


## Requirements

- Tested on Ubuntu / CentOS
- Tested on Ceph Firefly
- Requires iRODS >= 4.0.3

## Installation 

Currently, there are no prebuilt packages, but Ubuntu 12.04 and CentOS6.5 have been successfully tested.

Prerequisites for Ubuntu:

Follow the steps at http://docs.ceph.com/docs/master/start/quick-start-preflight/#advanced-package-tool-apt to add the official ceph repositories that match your running cluster's version.

```
apt-get install uuid-dev libssl-dev build-essential

apt-get install librados2 librados-dev

sudo dpkg -i librados*.deb
sudo apt-get install -f
```

Then checkout, build and install the plugin on the resource server

```
git clone https://github.com/meatz/irods_resource_plugin_rados.git
cd irods_resource_plugin_rados
make
make install
```

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
