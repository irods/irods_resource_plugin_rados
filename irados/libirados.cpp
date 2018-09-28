// #include "libirados.hpp"

// =-=-=-=-=-=-=-
// irods includes
#include "msParam.h"
#include "rcConnect.h"
#include "miscServerFunct.hpp"

// =-=-=-=-=-=-=-
#include "irods_resource_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_physical_object.hpp"
#include "irods_collection_object.hpp"
#include "irods_string_tokenize.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_stacktrace.hpp"
#include "irods_server_properties.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/function.hpp>
#include <boost/any.hpp>
#include <boost/thread.hpp>

// =-=-=-=-=-=-=-
// system includes
#ifndef _WIN32
#include <sys/file.h>
#include <sys/param.h>
#endif
#include <errno.h>
#include <sys/stat.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#if defined(osx_platform)
#include <sys/malloc.h>
#else
#include <malloc.h>
#endif
#include <fcntl.h>
#ifndef _WIN32
#include <sys/file.h>
#include <unistd.h>
#endif
#include <dirent.h>

#if defined(solaris_platform)
#include <sys/statvfs.h>
#endif
#if defined(linux_platform)
#include <sys/vfs.h>
#endif
#include <sys/stat.h>

#include <string.h>
#include <uuid/uuid.h>


// for ltoa
#include <stdio.h>
#include <stdlib.h>


// switches to turn on some debugging infos and verbose log output
//#define IRADOS_DEBUG

#ifdef IRADOS_DEBUG
    #include <unistd.h>
#endif


// RADOS
#include <rados/librados.hpp>

struct rados_conn_t {
    rados_t cluster_;
    rados_ioctx_t io_ctx_;
};

// 4MB blobs will provide best throughput
const uint64_t RADOS_BLOB_SIZE = 4194304;

boost::mutex rados_guard_;
boost::mutex propmap_guard_;

std::map<std::string, rados_conn_t*> rados_connections_;

#ifdef IRADOS_DEBUG
// just for debugging purposes
int num_open_fds_ = 0;
#endif

int _last_valid_fd = 0;


// store oid metadata between calls
std::map<std::string, int> oids_open_fds_cnt_;
std::map<std::string, bool> dirty_oids_;
std::map<std::string, uint64_t> oid_max_file_sizes_;
std::map<std::string, uint64_t> oid_num_blobs_;

// store file descriptor states between calls
std::map<int, uint64_t> fd_offsets_;
std::map<int, rados_ioctx_t> fd_contexts_;

/**
 * Create a unique object id that is used as the key in rados.
 */
std::string rand_uuid_string()
{
    uuid_t t;
    uuid_generate(t);

    char ch[36];
    memset(ch, 0, 36);
    uuid_unparse(t, ch);
    return std::string(ch);
}


/**
 * Create the rados connection.
 * Is called before each rados operation.
 * One global ioctx is shared by all operations.
 */
rados_ioctx_t* get_rados_ctx(const std::string context) {

    // get the cluster config string from resource context:

    #ifdef IRADOS_DEBUG
        rodsLog(LOG_NOTICE, "IRADOS_DEBUG CTX INIT -> %s", context.c_str());
    #endif

    rados_guard_.lock();

    struct rados_conn_t *rc = rados_connections_[context];

    if (rc != NULL) {
        rados_guard_.unlock();
        return &rc->io_ctx_;
    }

    // we need to set up a new radosconnection:

    rc = (rados_conn_t *) malloc(sizeof(struct rados_conn_t));

    std::string cluster_name = "ceph";
    std::string pool_name = "irods";
    std::string user_name = "client.irods";
    std::string config_file_path = "/etc/irods/irados.config";

    if (context.length() > 0) {
        int left = context.find("|");
        int right = context.rfind("|");

        cluster_name = context.substr(0, left);
        pool_name = context.substr(left + 1, (right - left - 1) );
        user_name = context.substr(right + 1, context.size());
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s parsed resource context as: cluster_name: %s, pool_name: %s, user_name: %s",
             __func__,
             cluster_name.c_str(),
             pool_name.c_str(),
             user_name.c_str());
        #endif
    }

     uint64_t flags = 0;

    int ret;
    /* Initialize the cluster handle */
    {
        ret = rados_create2(&rc->cluster_, cluster_name.c_str(), user_name.c_str(), flags);
        if (ret < 0) {
            rodsLog(LOG_ERROR, "Couldn't initialize the cluster handle! error %d", ret);
            return NULL;
        } else {
            #ifdef IRADOS_DEBUG
                rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s Created a new cluster handle", __func__);
            #endif
        }
    }

    /* Read the Ceph configuration file to configure the cluster handle. */
    {
        ret = rados_conf_read_file(rc->cluster_, config_file_path.c_str());
        if (ret < 0) {
            rodsLog(LOG_ERROR, "Couldn't read the Ceph configuration file! error %d", ret);
            return NULL;
        }
    }

    /* Connect to the cluster */
    {
        ret = rados_connect(rc->cluster_);
        if (ret < 0) {
            rodsLog(LOG_ERROR, "Couldn't connect to the cluster! error %d", ret);
            return NULL;
        } else {
            #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s Connected to the cluster.", __func__);
            #endif
        }
    }

    rados_ioctx_create(rc->cluster_, pool_name.c_str(), &rc->io_ctx_);

    if (ret < 0) {
        rodsLog(LOG_ERROR, "irados: cannot setup ioctx for cluster: error %d", ret);
        return NULL;
    }

    if (ret < 0) {
        rados_guard_.unlock();
        return NULL;
    }

    rados_connections_[context] = rc;
    rados_guard_.unlock();
    return &rc->io_ctx_;

}  // end of init_context


/**
 * Returns next free fd.
 * During the plugin lifetime, multiple fds may be opened.
 * Simply return a fresh one every time.
 */
int get_next_fd() {
    return ++_last_valid_fd;
}

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file registration
    irods::error irados_registered_plugin(
        irods::plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        #endif

        return result;
    }

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file unregistration
    irods::error irados_unregistered_plugin(
        irods::plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        return result;
    }

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file modification
    irods::error irados_modified_plugin(
        irods::plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        #endif

        return result;
    }

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file operation
    irods::error irados_notify_plugin(
        irods::plugin_context& _ctx,
        const std::string* _opr ) {
        irods::error result = SUCCESS();

        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        #endif

        return result;
    }


    // =-=-=-=-=-=-=-
    // interface to determine free space on a device given a path
    irods::error irados_get_fsfreespace_plugin(
        irods::plugin_context& _ctx ) {
        irods::error result = SUCCESS();

#ifdef IRADOS_DEBUG
    rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s called", __func__);
#endif


// TODO:  http://ceph.com/docs/master/rados/api/librados/
// int rados_cluster_stat(rados_t cluster, struct rados_cluster_stat_t *result)
// Read usage info about the cluster.
//
// This tells you total space, space used, space available, and number of objects. These are not updated immediately when data is written, they are eventually consistent.
//
// Parameters:
// cluster – cluster to query
// result – where to store the results
// Returns:
// 0 on success, negative error code on failure
//
// struct rados_cluster_stat_t {
//  uint64_t kb, kb_used, kb_avail;
//  uint64_t num_objects;
// };
//struct cluster_stat_t {
    //uint64_t kb, kb_used, kb_avail;
    //uint64_t num_objects;
  //};


    rodsLong_t fssize = 999999999999999;
    result.code( fssize );

    #ifdef IRADOS_DEBUG
        rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s leave", __func__);
    #endif
        return result;

    } // irados_get_fsfreespace_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX create
    irods::error irados_create_plugin(
        irods::plugin_context& _ctx ) {

#ifdef IRADOS_DEBUG
        rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        // irods::stacktrace st;
        // st.trace();
        // st.dump();
#endif
        irods::error result = SUCCESS();

        // create the irados internal id for this data object
        std::string oid = rand_uuid_string();


        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

        std::string context;
        _ctx.prop_map().get<std::string>(irods::RESOURCE_CONTEXT, context);

        fop->physical_path(oid);
        // irods::error ret = irados_get_fsfreespace_plugin( _ctx );
        // if ( ( result = ASSERT_PASS( ret, "Error determining freespace on system." ) ).ok() ) {
        //     rodsLong_t file_size = fop->size();
        //     if ( ( result = ASSERT_ERROR( file_size < 0 || ret.code() >= file_size, USER_FILE_TOO_LARGE, "File size: %ld is greater than space left on device: %ld", file_size, ret.code() ) ).ok() ) {
        //
        //     } else {
        //         result.code( PLUGIN_ERROR );
        //         return result;
        //     }
        // }


        const auto* io_ctx_ptr = get_rados_ctx(context);
        if (io_ctx_ptr == NULL) {
            rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - no io_ctx_t available", __func__, oid.c_str());
            result.code(PLUGIN_ERROR);
            return result;
        }
        const auto& io_ctx = *io_ctx_ptr;

        propmap_guard_.lock();

        int fd = get_next_fd();
        fop->file_descriptor(fd);

        // creates and sets an initial seek ptr for the current fd.
        // _ctx.prop_map().set < uint64_t > ("OFFSET_PTR_" + fd, 0);
        fd_offsets_[fd] = 0;

        fd_contexts_[fd] = io_ctx;

        // This is the first!
        oids_open_fds_cnt_[oid] = 1;
        dirty_oids_[oid] = true;

        propmap_guard_.unlock();

        #ifdef IRADOS_DEBUG
            int flags = fop->flags();
            num_open_fds_++;

            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s created: %s with flags: %d - (fd: %d) open fds: %d, context: %s",
            __func__,
            oid.c_str(),
            flags,
            fd,
            num_open_fds_,
            context.c_str());
        #endif

        return result;
    } // irados_create_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error irados_open_plugin(
        irods::plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

        std::string oid = fop->physical_path();

        int flags = fop->flags();

        std::string context;
        _ctx.prop_map().get<std::string>(irods::RESOURCE_CONTEXT, context);


#ifdef IRADOS_DEBUG
        rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s called on oid: %s for path: %s with flags: %d, context: %s",
            __func__,
            oid.c_str(),
            fop->physical_path().c_str(),
            flags,
            context.c_str()
            );
#endif

        const auto* io_ctx_ptr = get_rados_ctx(context);
        if (io_ctx_ptr == NULL) {
            rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - no io_ctx_t available", __func__, oid.c_str());
            result.code(PLUGIN_ERROR);
            return result;
        }
        const auto& io_ctx = *io_ctx_ptr;

        propmap_guard_.lock();
        int fd = get_next_fd();
        fop->file_descriptor(fd);

        // creates and sets an initial seek ptr.
        fd_offsets_[fd] = 0;
        fd_contexts_[fd] = io_ctx;

        #ifdef IRADOS_DEBUG
            num_open_fds_++;
            std::string lid = fop->logical_path();
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s opened: oid: %s logical: %s - (fd: %d) open fds: %d | on file %d",
                __func__,
                oid.c_str(),
                lid.c_str(),
                fd,
                num_open_fds_,
                oids_open_fds_cnt_[oid]);
        #endif

        propmap_guard_.unlock();


        if (flags & O_RDONLY) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_RDONLY", fop->physical_path().c_str());
        }
        if (flags & O_WRONLY) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_WRONLY", fop->physical_path().c_str());
        }
        if (flags & O_RDWR) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_RDWR", fop->physical_path().c_str());
        }
        if (flags & O_TRUNC) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_TRUNC", fop->physical_path().c_str());
        }
        if (flags & O_APPEND) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_APPEND", fop->physical_path().c_str());
        }
        if (flags & O_CREAT) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_CREAT", fop->physical_path().c_str());
        }
        if (flags & O_LARGEFILE) {
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG open FLAGS path: %s has O_LARGEFILE", fop->physical_path().c_str());
        }


        if ((flags & O_TRUNC) && (flags & O_CREAT) && (flags & O_RDWR)) {
            // File was opened for overwrite. Therefore, truncate it first and handle it as if it was called by create_plugin().

            // how many blobs do we have? delete all... Essentially this comes close to a truncate(0).
            char num_blobs_str[32];
            int r = rados_getxattr(io_ctx, oid.c_str(), "NUM_BLOBS", num_blobs_str, sizeof(num_blobs_str));

            if (r < 0) {
                rodsLog(LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s rados_getxattr returned: %d", __func__, oid.c_str(), r);
                result.code(UNIX_FILE_STAT_ERR);
                return result;
            }

            uint64_t num_blobs = strtoul(num_blobs_str, NULL, 0);

            int status = 0;
            for (uint64_t blob_id = 0; blob_id <= num_blobs; blob_id++) {

                std::stringstream blob_oid;
                if (blob_id > 0) {
                    blob_oid << oid << "-" << blob_id;
                } else {
                    // the first block has no -XX identifier
                    blob_oid << oid;
                }

                status = rados_remove(io_ctx, blob_oid.str().c_str());
                if (status != 0) {
                    rodsLog(LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s remove(%s) returned: %d",
                     __func__,
                     oid.c_str(),
                     blob_oid.str().c_str(),
                     status);
                    result.code( status );
                    return result;
                }
            }
        }


        // Retrieve object stats like total object size, etc. They are stored in the first blob's xattrs.

        // fd_cnt_for_this_object++
        // int cnt = ++oids_open_fds_cnt_[oid];

        // if (cnt == 1) {
        //     // NOTE: This must only happen, if this fs is the first for the oid.

        //     char xfilesize[32];
        //     int r = rados_getxattr(io_ctx, oid.c_str(), "FILE_SIZE", xfilesize, sizeof(xfilesize));

        //     if (r < 0) {
        //         rodsLog(LOG_ERROR, "IRADOS_DEBUG %s (%s) -> oid: %s rados_getxattr(FILE_SIZE) returned: %d",
        //             __func__,
        //             pool_name_.c_str(),
        //             oid.c_str(),
        //             r);
        //         result.code(PLUGIN_ERROR);
        //         return result;
        //     }
        //     else {
        //         uint64_t file_size = strtoul(xfilesize, NULL, 0);
        //         _ctx.prop_map().set<uint64_t>("SIZE_" + oid, file_size);
        //     }

        // }

        return result;

    } // irados_open_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error irados_read_plugin(
        irods::plugin_context& _ctx,
        void* _buf,
        const int _len) {
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        std::string oid = fop->physical_path();
        int fd = fop->file_descriptor();

        int status = 0;

        propmap_guard_.lock();

        uint64_t read_ptr = fd_offsets_[fd];
        rados_ioctx_t io_ctx = fd_contexts_[fd];


        // make sure to not read out of the bounds of this object.
        // uint64_t max_file_size = 0;
        // _ctx.prop_map().get < uint64_t > ("SIZE_" + oid, max_file_size);

        propmap_guard_.unlock();

        // check that only existing bytes are read.

        // if (read_ptr >= max_file_size) {
        //     result.code(0);
        //     return result;
        // }

        // if (read_ptr + _len > max_file_size) {
        //   _len = (max_file_size - read_ptr);
        // }


        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s ReadRequest: from %s off: %lu, len: %d - (fd: %d)",
                __func__,
                oid.c_str(),
                read_ptr,
                _len,
                fd);
        #endif

        uint64_t bytes_read = 0;

        while (bytes_read < _len) {
            uint64_t blob_id = (read_ptr + bytes_read) / RADOS_BLOB_SIZE;
            uint64_t blob_offset = (read_ptr + bytes_read) % RADOS_BLOB_SIZE;
            uint64_t read_len = std::min((_len - bytes_read), (RADOS_BLOB_SIZE - blob_offset));

            // determine the correct object id of the block to write
            std::stringstream blob_oid;
            if (blob_id > 0) {
                blob_oid << oid << "-" << blob_id;
            } else {
                // the first block has no -XX identifier
                blob_oid << oid;
            }

            char* read_buf = (char*) _buf;
            read_buf += bytes_read;
            status = rados_read(io_ctx, blob_oid.str().c_str(), read_buf, read_len, blob_offset);

            if (status < 0) {
                rodsLog(LOG_ERROR, "Couldn't read object '%s' - error: %d",
                 blob_oid.str().c_str(),
                 status);
                    result.code(UNIX_FILE_OPEN_ERR);
                    return result;
            }

            if (status == 0) {
                // no bytes read, so the file might have ended.
                break;
            }

            bytes_read += status;

#ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s ReadPart: from %s blob_id: %d, blob_offset: %lu, read: %lu, rados_read status: %d, (fd: %d)",
                    __func__,
                    blob_oid.str().c_str(),
                    blob_id,
                    blob_offset,
                    read_len,
                    status,
                    fd);
#endif
        }


        result.code(bytes_read);

        propmap_guard_.lock();

        fd_offsets_[fd] = read_ptr + bytes_read;
        propmap_guard_.unlock();

        return result;
    } // irados_read_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error irados_write_plugin(
        irods::plugin_context& _ctx,
        const void* _buf,
        const int _len) {

        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        std::string oid = fop->physical_path();
        int fd = fop->file_descriptor();

        propmap_guard_.lock();
        uint64_t write_ptr = fd_offsets_[fd];
        rados_ioctx_t io_ctx = fd_contexts_[fd];
        propmap_guard_.unlock();

        // each irods file is split into multiple 4mb blobs that are stored in rados.
        // for the given offset + length, find the correct blob-id
        uint64_t bytes_written = 0;
        uint64_t blob_id = 0;

        while (bytes_written < _len) {
            blob_id = (write_ptr + bytes_written) / RADOS_BLOB_SIZE;
            uint64_t blob_offset = (write_ptr + bytes_written) % RADOS_BLOB_SIZE;

            uint64_t write_len = std::min((_len - bytes_written), (RADOS_BLOB_SIZE - blob_offset));

            const char* write_buf = (char*)_buf;
            write_buf += bytes_written;

            // determine the correct object id of the block to write
            std::stringstream out;
            if (blob_id > 0) {
                out << oid << "-" << blob_id;
            } else {
                // the first block has no -XX identifier
                out << oid;
            }
            std::string blob_oid = out.str();

            int status = rados_write(io_ctx, blob_oid.c_str(), write_buf, write_len, blob_offset);

            if (status < 0) {
                rodsLog(LOG_ERROR, "Couldn't write object '%s' - error: %d", oid.c_str(), status);
                result.code(PLUGIN_ERROR);
                return result;
            }
            #ifdef IRADOS_DEBUG
                rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s to %s blob_id: %d, blob_offset: %lu, write_len: %lu, rados_write_status: %d, (fd: %d)",
                    __func__,
                    blob_oid.c_str(),
                    blob_id,
                    blob_offset,
                    write_len,
                    status,
                    fd);
            #endif

            bytes_written += write_len;
        }

        propmap_guard_.lock();

        dirty_oids_[oid] = true;

        fd_offsets_[fd] = write_ptr + bytes_written;

        // keep track of the highest offset for this file as it marks the actual file size.
        // finally, the file size will be written as the base object's xattr "object_size"
        uint64_t max_file_size = oid_max_file_sizes_["SIZE_" + oid];


        if ((write_ptr + _len) > max_file_size) {
            oid_max_file_sizes_["SIZE_" + oid] = (write_ptr + _len);
        }

        // keep track of the number of total blobs of the file.
        // will be marked in the base objects xattr as well.

        uint64_t num_blobs = oid_num_blobs_["NUM_BLOBS_" + oid];

        if (blob_id > num_blobs) {
            oid_num_blobs_["NUM_BLOBS_" + oid] = blob_id;
        }

        propmap_guard_.unlock();
        result.code( _len );
        return result;
    } // irados_write_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error irados_close_plugin(
        irods::plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif
        irods::error result = SUCCESS();
        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());

        std::string oid = fop->physical_path();
        int fd = fop->file_descriptor();

        rados_ioctx_t io_ctx = fd_contexts_[fd];


        propmap_guard_.lock();

        int fd_cnt = oids_open_fds_cnt_[oid];

        if (fd_cnt > 1) {
            oids_open_fds_cnt_[oid] -= 1;
            propmap_guard_.unlock();
        } else {
            // this was the last fd for the object.
            oids_open_fds_cnt_.erase(oid);

            // check if any of the opened fds actually changed something.
            bool dirty = dirty_oids_[oid];

            if (not dirty) {
                propmap_guard_.unlock();
            } else {
                #ifdef IRADOS_DEBUG
                    rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s - %s - fd_cnt: %d Writing metadata for dirty oid",
                        __func__,
                        oid.c_str(),
                        fd_cnt);
                #endif

                uint64_t num_blobs = oid_num_blobs_["NUM_BLOBS_" + oid];

                uint64_t max_file_size = oid_max_file_sizes_["SIZE_" + oid];

                // this was the last fd for the file. now persist, num_blobs and file_size to the base oid object.
                propmap_guard_.unlock();

                std::stringstream xfilesize;
                xfilesize << max_file_size;

                int r = rados_setxattr(io_ctx, oid.c_str(), "FILE_SIZE", xfilesize.str().c_str(), xfilesize.str().size());
                if (r > 0) {
                    rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - error during rados_setxattr()", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                }

                std::stringstream xnumblobs;
                xnumblobs << num_blobs;

                r = rados_setxattr(io_ctx, oid.c_str(), "NUM_BLOBS", xnumblobs.str().c_str(), xnumblobs.str().size());
                if (r > 0) {
                    rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - error during rados_setxattr()", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                }

                std::stringstream xblobsize;
                xblobsize << RADOS_BLOB_SIZE;

                r = rados_setxattr(io_ctx, oid.c_str(), "BLOB_SIZE", xblobsize.str().c_str(), xblobsize.str().size());
                if (r > 0) {
                    rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - error during rados_setxattr()", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                }

                #ifdef IRADOS_DEBUG
                     rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s %s - closed blob: highest blob-id: %lu, size: %lu (fd: %d)",
                    __func__,
                    oid.c_str(),
                    num_blobs,
                    max_file_size,
                    fd,
                    num_open_fds_);
                #endif
            }
        }

        return result;
    } // irados_close_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error irados_unlink_plugin(
        irods::plugin_context& _ctx ) {

        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        std::string oid = fop->physical_path();

        std::string context;
        _ctx.prop_map().get<std::string>(irods::RESOURCE_CONTEXT, context);


#ifdef IRADOS_DEBUG
        rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s called on oid: %s with path: %s",
         __func__,
         oid.c_str(),
         fop->physical_path().c_str());
#endif

        const auto* io_ctx_ptr = get_rados_ctx(context);
        if (io_ctx_ptr == NULL) {
            rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - no io_ctx_t available", __func__, oid.c_str());
            result.code(PLUGIN_ERROR);
            return result;
        }
        const auto& io_ctx = *io_ctx_ptr;

        // how many blobs do we have?
        char num_blobs_str[32];
        int r = rados_getxattr(io_ctx, oid.c_str(), "NUM_BLOBS", num_blobs_str, sizeof(num_blobs_str));

        if (r < 0) {
            rodsLog(LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s rados_getxattr returned: %d", __func__, oid.c_str(), r);
            result.code(PLUGIN_ERROR);
            return result;
        }

        uint64_t num_blobs = strtoul(num_blobs_str, NULL, 0);

        int status = 0;
        for (uint64_t blob_id = 0; blob_id <= num_blobs; blob_id++) {

        std::stringstream blob_oid;
            if (blob_id > 0) {
                blob_oid << oid << "-" << blob_id;
            } else {
                // the first block has no -XX identifiert
                blob_oid << oid;
            }

            status = rados_remove(io_ctx, blob_oid.str().c_str());
            if (status != 0) {
                rodsLog(LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s rados_remove(%s) returned: %d", __func__, oid.c_str(), blob_oid.str().c_str(), status);
            }
            result.code( status );
        }

        #ifdef IRADOS_DEBUG

            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s %s removed %d blobs",
                __func__,
                oid.c_str(),
                num_blobs);
        #endif

        return result;
    } // irados_unlink_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error irados_stat_plugin(
        irods::plugin_context& _ctx,
        struct stat* _statbuf ) {
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string oid = fop->physical_path();

        std::string context;
        _ctx.prop_map().get<std::string>(irods::RESOURCE_CONTEXT, context);

        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s -> oid: %s",
                __func__,
                oid.c_str());
        #endif

        const auto* io_ctx_ptr = get_rados_ctx(context);
        if (io_ctx_ptr == NULL) {
            rodsLog(LOG_ERROR, "IRADOS_DEBUG %s - %s - no io_ctx_t available", __func__, oid.c_str());
            result.code(PLUGIN_ERROR);
            return result;
        }
        const auto& io_ctx = *io_ctx_ptr;


        uint64_t psize;
        time_t pmtime;

        int status = rados_stat(io_ctx, oid.c_str(), &psize, &pmtime);

        char xfilesize[32];
        int r = rados_getxattr(io_ctx, oid.c_str(), "FILE_SIZE", xfilesize, sizeof(xfilesize));

        if (r < 0) {
            rodsLog(LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s rados_getxattr(FILE_SIZE) returned: %d",
                __func__,
                oid.c_str(),
                r);
            result.code(PLUGIN_ERROR);
            return result;
        }

        uint64_t file_size = strtoul(xfilesize, NULL, 0);

        _statbuf->st_mtime = pmtime;
        _statbuf->st_size = file_size;
        _statbuf->st_mode = 0 | S_IRWXU | S_IRWXG;

         #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s -> status: %d, oid: %s stat got pmtime: %lu, size:%lu",
                __func__,
                status,
                oid.c_str(),
                pmtime,
                file_size);
        #endif

        // result.code(status);


        return result;
    } // irados_stat_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error irados_lseek_plugin(
        irods::plugin_context& _ctx,
        long long                           _offset,
        int                                 _whence ) { // always is SEEK_SET == 0
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        int fd = fop->file_descriptor();

        propmap_guard_.lock();
        fd_offsets_[fd] = _offset;
        propmap_guard_.unlock();

        result.code(_offset);

        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s offset: %lu, whence: %d - (fd: %d)",
            __func__,
            _offset,
            _whence,
            fd);
        #endif

        return result;
    } // irados_lseek_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error irados_mkdir_plugin(
        irods::plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif

        // we do not know any directory.
        return SUCCESS();
    } // irados_mkdir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX rmdir
    irods::error irados_rmdir_plugin(
        irods::plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif
        // we do not know any directory.
        return SUCCESS();
    } // irados_rmdir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error irados_opendir_plugin(
        irods::plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
         return ERROR( SYS_NOT_SUPPORTED, "irados_opendir_plugin" );
    } // irados_opendir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error irados_closedir_plugin(
        irods::plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
        return ERROR(SYS_NOT_SUPPORTED, "irados_closedir_plugin" );
    } // irados_closedir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error irados_readdir_plugin(
        irods::plugin_context& _ctx,
        struct rodsDirent** _dirent_ptr ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
        return ERROR( SYS_NOT_SUPPORTED, "irados_readdir_plugin" );
    } // irados_readdir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error irados_rename_plugin(
        irods::plugin_context& _ctx,
        const char* _new_file_name ) {

        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif

        // rename in rados is a: get old_oid, put old_oid new_oid, rm old_oid
        // we have a fixed physical path and do not reflect changes to the physical path.
        return SUCCESS();
    } // irados_rename_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error irados_truncate_plugin(
        irods::plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
        return ERROR(SYS_NOT_SUPPORTED, "irados_truncate_plugin" );
    } // irados_truncate_plugin

    // =-=-=-=-=-=-=-
    // redirect_create - code to determine redirection for create operation
    irods::error irados_redirect_create(
        irods::plugin_property_map&   _prop_map,
        irods::file_object_ptr        _file_obj,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {
        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"status\" property." ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN == resc_status ) {
                _out_vote = 0.0;
                result.code( SYS_RESC_IS_DOWN );
                // result = PASS( result );
            }
            else {

                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"location\" property." ) ).ok() ) {

                    // =-=-=-=-=-=-=-
                    // vote higher if we are on the same host
                    if ( _curr_host == host_name ) {
                        _out_vote = 1.0;
                    }
                    else {
                        _out_vote = 0.5;
                    }
                }
            }
        }
#ifdef IRADOS_DEBUG
    rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s leave", __func__);
#endif
        return result;

    } // irados_redirect_create

//     =-=-=-=-=-=-=-
//     redirect_open - code to determine redirection for open operation
    irods::error irados_redirect_open(
        irods::plugin_property_map&   _prop_map,
        irods::file_object_ptr        _file_obj,
        const std::string&             _resc_name,
        const std::string&             _curr_host,
        float&                         _out_vote ) {
        irods::error result = SUCCESS();

        // =-=-=-=-=-=-=-
        // initially set a good default
        _out_vote = 0.0;

        // =-=-=-=-=-=-=-
        // determine if the resource is down
        int resc_status = 0;
        irods::error get_ret = _prop_map.get< int >( irods::RESOURCE_STATUS, resc_status );
        if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"status\" property." ) ).ok() ) {

            // =-=-=-=-=-=-=-
            // if the status is down, vote no.
            if ( INT_RESC_STATUS_DOWN != resc_status ) {

                // =-=-=-=-=-=-=-
                // get the resource host for comparison to curr host
                std::string host_name;
                get_ret = _prop_map.get< std::string >( irods::RESOURCE_LOCATION, host_name );
                if ( ( result = ASSERT_PASS( get_ret, "Failed to get \"location\" property." ) ).ok() ) {

                    // =-=-=-=-=-=-=-
                    // set a flag to test if were at the curr host, if so we vote higher
                    bool curr_host = ( _curr_host == host_name );

                    // =-=-=-=-=-=-=-
                    // make some flags to clarify decision making
                    bool need_repl = ( _file_obj->repl_requested() > -1 );

                    // =-=-=-=-=-=-=-
                    // set up variables for iteration
                    irods::error final_ret = SUCCESS();
                    std::vector< irods::physical_object > objs = _file_obj->replicas();
                    std::vector< irods::physical_object >::iterator itr = objs.begin();

                    // =-=-=-=-=-=-=-
                    // check to see if the replica is in this resource, if one is requested
                    for ( ; itr != objs.end(); ++itr ) {
                        // =-=-=-=-=-=-=-
                        // run the hier string through the parser and get the last
                        // entry.
                        std::string last_resc;
                        irods::hierarchy_parser parser;
                        parser.set_string( itr->resc_hier() );
                        parser.last_resc( last_resc );

                        // =-=-=-=-=-=-=-
                        // more flags to simplify decision making
                        bool repl_us  = ( _file_obj->repl_requested() == itr->repl_num() );
                        bool resc_us  = ( _resc_name == last_resc );
                        bool is_dirty = ( itr->is_dirty() != 1 );

                        // =-=-=-=-=-=-=-
                        // success - correct resource and don't need a specific
                        //           replication, or the repl nums match
                        if ( resc_us ) {
                            // =-=-=-=-=-=-=-
                            // if a specific replica is requested then we
                            // ignore all other criteria
                            if ( need_repl ) {
                                if ( repl_us ) {
                                    _out_vote = 1.0;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // repl requested and we are not it, vote
                                    // very low
                                    _out_vote = 0.25;
                                }
                            }
                            else {
                                // =-=-=-=-=-=-=-
                                // if no repl is requested consider dirty flag
                                if ( is_dirty ) {
                                    // =-=-=-=-=-=-=-
                                    // repl is dirty, vote very low
                                    _out_vote = 0.25;
                                }
                                else {
                                    // =-=-=-=-=-=-=-
                                    // if our repl is not dirty then a local copy
                                    // wins, otherwise vote middle of the road
                                    if ( curr_host ) {
                                        _out_vote = 1.0;
                                    }
                                    else {
                                        _out_vote = 0.5;
                                    }
                                }
                            }

                            break;

                        } // if resc_us

                    } // for itr
                }
            }
            else {
                result.code( SYS_RESC_IS_DOWN );
                result = PASS( result );
            }
        }
#ifdef IRADOS_DEBUG
    rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s leave", __func__);
#endif
        return result;

    } // irados_redirect_open


     // =-=-=-=-=-=-=-
     // used to allow the resource to determine which host
     // should provide the requested operation
     irods::error irados_resolve_hierarchy_plugin(
         irods::plugin_context& _ctx,
         const std::string* _opr,
         const std::string* _curr_host,
         irods::hierarchy_parser* _out_parser,
         float* _out_vote ) {
         irods::error result = SUCCESS();

         rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s called %s -> %s", __func__, _opr->c_str(), _curr_host->c_str());


         // =-=-=-=-=-=-=-
         // check the context validity
         irods::error ret = _ctx.valid< irods::file_object >();
         if ( ( result = ASSERT_PASS( ret, "Invalid resource context." ) ).ok() ) {

             // =-=-=-=-=-=-=-
             // check incoming parameters
             if ( ( result = ASSERT_ERROR( _opr && _curr_host && _out_parser && _out_vote, SYS_INVALID_INPUT_PARAM, "Invalid input parameter." ) ).ok() ) {
                 // =-=-=-=-=-=-=-
                 // cast down the chain to our understood object type
                 irods::file_object_ptr file_obj = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );

                 // =-=-=-=-=-=-=-
                 // get the name of this resource
                 std::string resc_name;
                 ret = _ctx.prop_map().get< std::string >( irods::RESOURCE_NAME, resc_name );
                 if ( ( result = ASSERT_PASS( ret, "Failed in get property for name." ) ).ok() ) {
                     // =-=-=-=-=-=-=-
                     // add ourselves to the hierarchy parser by default
                     _out_parser->add_child( resc_name );

                     // =-=-=-=-=-=-=-
                     // test the operation to determine which choices to make
                     if ( irods::OPEN_OPERATION == ( *_opr ) ||
                         irods::WRITE_OPERATION == ( *_opr ) ||
                         irods::UNLINK_OPERATION == ( *_opr ) ) {
                         // =-=-=-=-=-=-=-
                         // call redirect determination for 'get' operation
                         ret = irados_redirect_open( _ctx.prop_map(), file_obj, resc_name, ( *_curr_host ), ( *_out_vote ) );
                         result = ASSERT_PASS( ret, "Failed redirecting for open." );

                     }
                     else if ( irods::CREATE_OPERATION == ( *_opr ) ) {
                         // =-=-=-=-=-=-=-
                         // call redirect determination for 'create' operation
                         ret = irados_redirect_create( _ctx.prop_map(), file_obj, resc_name, ( *_curr_host ), ( *_out_vote ) );
                         result = ASSERT_PASS( ret, "Failed redirecting for create." );
                     }

                     else {
                         // =-=-=-=-=-=-=-
                         // must have been passed a bad operation
                         result = ASSERT_ERROR( false, INVALID_OPERATION, "Operation not supported." );
                     }
                 }
             }
         }

         return result;

     } // example_file_redirect_plugin

     // =-=-=-=-=-=-=-
     // example_file_rebalance - code which would rebalance the subtree
     irods::error irados_rebalance(
         irods::plugin_context& _ctx ) {
         return SUCCESS();

     } // example_file_rebalancec



    class irados_resource : public irods::resource {

    public:
        irados_resource(
            const std::string& _inst_name,
            const std::string& _context ) :
            irods::resource(
                _inst_name,
                _context ) {
#ifdef IRADOS_DEBUG
    rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s CTOR (%s, %s)", __func__, _inst_name.c_str(), _context.c_str());
#endif
        } // ctor


//        // =-=-=-=-=-=-=-
//        // 3b. pass along a functor for maintenance work after
//        //     the client disconnects, uncomment the first two lines for effect.
        irods::error post_disconnect_maintenance_operation( irods::pdmo_type& _op ) {
            irods::error result = SUCCESS();

            #ifdef IRADOS_DEBUG
                rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s POST DISCONNECT HOOK", __func__);
            #endif
            return result;
        }
    }; // class irados_resource

    // =-=-=-=-=-=-=-
    // 4. create the plugin factory function which will return a dynamically
    //    instantiated object of the previously defined derived resource.  use
    //    the add_operation member to associate a 'call name' to the interfaces
    //    defined above.  for resource plugins these call names are standardized
    //    as used by the irods facing interface defined in
    //    server/drivers/src/fileDriver.c
extern "C"
irods::resource* plugin_factory(const std::string& _inst_name,
        const std::string& _context) {

    // =-=-=-=-=-=-=-
    // 4a. create irados_resource
    irados_resource* resc = new irados_resource(_inst_name, _context);

    // =-=-=-=-=-=-=-
    // 4b. map function names to operations.  this map will be used to load
    //     the symbols from the shared object in the delay_load stage of
    //     plugin loading.
    resc->add_operation(irods::RESOURCE_OP_CREATE, &irados_create_plugin);
    resc->add_operation(irods::RESOURCE_OP_OPEN, &irados_open_plugin);
    resc->add_operation<void*,int>(irods::RESOURCE_OP_READ, std::function<irods::error(irods::plugin_context&,void*,int)>(irados_read_plugin));
    resc->add_operation<const void*,int>(irods::RESOURCE_OP_WRITE, std::function<irods::error(irods::plugin_context&,const void*,int)>(irados_write_plugin));
    resc->add_operation(irods::RESOURCE_OP_CLOSE, &irados_close_plugin);
    resc->add_operation(irods::RESOURCE_OP_UNLINK, &irados_unlink_plugin);
    resc->add_operation<struct stat*>(irods::RESOURCE_OP_STAT, std::function<irods::error(irods::plugin_context&, struct stat*)>(irados_stat_plugin));
    resc->add_operation<long long,int>(irods::RESOURCE_OP_LSEEK, std::function<irods::error(irods::plugin_context&, long long, int)>(irados_lseek_plugin));
    resc->add_operation(irods::RESOURCE_OP_MKDIR, &irados_mkdir_plugin);
    resc->add_operation(irods::RESOURCE_OP_RMDIR, &irados_rmdir_plugin);
    resc->add_operation(irods::RESOURCE_OP_OPENDIR, &irados_opendir_plugin);
    resc->add_operation(irods::RESOURCE_OP_CLOSEDIR, &irados_closedir_plugin);
    resc->add_operation<struct rodsDirent**>(irods::RESOURCE_OP_READDIR, std::function<irods::error(irods::plugin_context&,struct rodsDirent**)>(irados_readdir_plugin));
    resc->add_operation<const char*>(irods::RESOURCE_OP_RENAME, std::function<irods::error(irods::plugin_context&, const char*)>(irados_rename_plugin));
    resc->add_operation(irods::RESOURCE_OP_TRUNCATE, &irados_truncate_plugin);
    resc->add_operation(irods::RESOURCE_OP_FREESPACE, &irados_get_fsfreespace_plugin);

    resc->add_operation( irods::RESOURCE_OP_REGISTERED,   &irados_registered_plugin );
    resc->add_operation( irods::RESOURCE_OP_UNREGISTERED, &irados_unregistered_plugin );
    resc->add_operation( irods::RESOURCE_OP_MODIFIED,     &irados_modified_plugin );
    resc->add_operation<const std::string*>( irods::RESOURCE_OP_NOTIFY, std::function<irods::error(irods::plugin_context&, const std::string*)>(irados_notify_plugin) );

    resc->add_operation<const std::string*,const std::string*,irods::hierarchy_parser*,float*>(irods::RESOURCE_OP_RESOLVE_RESC_HIER, std::function<irods::error(irods::plugin_context&, const std::string*, const std::string*, irods::hierarchy_parser*, float*)>(irados_resolve_hierarchy_plugin));
    resc->add_operation(irods::RESOURCE_OP_REBALANCE, &irados_rebalance);

    // =-=-=-=-=-=-=-
    // set some properties necessary for backporting to iRODS legacy code
    resc->set_property<int>(irods::RESOURCE_CHECK_PATH_PERM, 2); //DO_CHK_PATH_PERM );
    resc->set_property<int>(irods::RESOURCE_CREATE_PATH, 1);    //CREATE_PATH );

    return dynamic_cast<irods::resource*>(resc);

} // plugin_factory
