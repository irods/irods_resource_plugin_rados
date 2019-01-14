# irods_resource_plugin_rados

Cacheless, direct Ceph/rados resource plugin for iRODS.

First presentation at iRODS user group meeting 2014 / Boston, MA - http://www.slideshare.net/mgrawinkel/development-of-the-irods-rados-plugin

## TL;DR

- `iadmin mkresc radosResc irados rs-host:/path "cluster_name|pool_name|client_name"`
- No superfluous cache/archive tier
- Parallel, direct, high performance access to your data!
- Multiple rados pools from one resource server

## Introduction

This iRODS plugin implements a direct access to Ceph/rados in the most efficient manner.
Files in the iRODS namespace are mapped to objects in the rados key-blob store.
In contrast to other plugins, the irados resource plugin does not need to cache or stage files, but gives you direct and parallel access to data.
Internally, the plugin maps the POSIX like open, read, write, seek, unlink, stat, and close calls to the librados client's operations.
To fully use the inherent rados cluster parallelity, irods files are split to multiple 4 MB files and uploads of large files open multiple parallel transfer threads.

The plugin assumes that file's ACLs as well as its namespace and metadata is fully managed by iRODS.
Rados stores the bytes of the file with the options of the target pool.

For every new file, a unique uuid is generated as the primary access key in rados. This uuid is set as the physical path of the file in the iRODS icat.
Files are spread to 4 MB blobs that are named by an incrementing identifier suffix.
The first object is called by its uuid and contains extended attributes to store the actual size of the file and the number of blobs that make it up.
All following files are named as uuid-1, uuid-2, ...

## Requirements

- Tested on Ubuntu / CentOS
- Requires iRODS >= 4.0.3

## Installation 

Currently, there are no prebuilt packages, but Ubuntu 12.04 and CentOS6.5 have been successfully tested.

Prerequisites for Ubuntu:

Follow the steps at http://docs.ceph.com/docs/master/start/quick-start-preflight/#advanced-package-tool-apt to add the official ceph repositories that match your running cluster's version.

```
sudo apt-get install uuid-dev libssl-dev build-essential
sudo apt-get install librados2 librados-dev
sudo apt-get install -f
```
Prerequisites for CentOS:

Follow the steps at http://docs.ceph.com/docs/master/start/quick-start-preflight/#rhel-centos to add the official ceph repositories that match your running cluster's version.

```
yum install librados2 librados2-devel libuuid-devel openssl-devel cmake3 irods-devel irods-externals-clang3.8-0
yum groups install "Development Tools"
```

Then checkout, build and install the plugin on the resource server:

```
git clone https://github.com/irods/irods_resource_plugin_rados.git
mkdir build_irods_resource_plugin_rados
cd build_irods_resource_plugin_rados
cmake ../irods_resource_plugin_rados # or cmake3 on CentOS
make
sudo make install
```

## Setup

Create an irods pool on ceph, i.e.

```
ceph osd pool create irods 128 
ceph auth get-or-create client.irods osd 'allow rw pool=irods' mon 'allow r' > /etc/ceph/client.irods.keyring
```

N.B: 128 is the "Placement Group", see http://docs.ceph.com/docs/mimic/rados/operations/placement-groups/

Copy the key from the newly created keyring and create the ceph config files on the resource server.
You can have multiple pools with different clients & capabilities.

`touch /etc/irods/irados.config && chown irods: /etc/irods/irados.config && chmod 600 /etc/irods/irados.config`

```
[global]
    mon host = ceph-mon

[client.irods]
        key = AQD7pVhUSMx1JRAA1eqDfSynx4qQBe9DHt79Ow==

[client.irods2]
        key = AQB3xHVUAPS+HxAA6PlML8jmcDMkX+5SP7Y6lw==
```

The cluster_name, pool_name, and user_name to connect to a rados pool are configured in the resource context on resource creation.

If no context like :/tmp/ is provided, the plugin does not work correctly. Nevertheless, the context is not used at all.
```
iadmin mkresc radosResc irados rs-host:/path "cluster_name|pool_name|client_name"
```

Then upload files with:

```
iput -R radosResc files/
```

## Scale out

All traffic from clients to rados is routed through the resource server. If it becomes a bottleneck, just add more!

```
iadmin mkresc radosRandomResc random
iadmin mkresc child_01 irados rs-01.local:/path "ceph|poolname|client.irods"
iadmin mkresc child_02 irados rs-02.local:/path "ceph|poolname|client.irods"
...
iadmin addchildtoresc radosRandomResc child_01
iadmin addchildtoresc radosRandomResc child_02
...
```

The `radosRandomResc` will then distribute the load over all resource servers.

