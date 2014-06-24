
// =-=-=-=-=-=-=-
// irods includes
#include "msParam.hpp"
#include "reGlobalsExtern.hpp"
#include "rcConnect.hpp"
#include "readServerConfig.hpp"
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

// RADOS
#include <rados/librados.hpp>

#define IRADOS_DEBUG

#ifdef IRADOS_DEBUG
    #include <time.h>
    #include "irods_stacktrace.hpp"
    #include <unistd.h>
#endif

#define IRADOS_TIME

#ifdef IRADOS_TIME
    #include <time.h>
    #include <iostream>
#endif


// TODO: make configurable
const char *pool_name = "irods";
const char cluster_name[] = "ceph";
const char user_name[] = "client.irods";


// 4MB blobs will provide best throughput
const uint64_t RADOS_BLOB_SIZE = 4194304;

boost::mutex rados_guard_;
boost::mutex propmap_guard_;

bool rados_initialized_ = false;

static librados::Rados* rados_cluster_ = NULL;


// just for debugging purposes
int num_open_fds_ = 0;

std::map<std::string, int> oids_open_fds_cnt_;
std::map<std::string, bool> dirty_oids_;
std::map<int, uint64_t> fd_offsets_;

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
    return string(ch);
}

bool connect_rados_cluster() {
    
#ifdef IRADOS_TIME
    timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 0;
    clock_settime(CLOCK_PROCESS_CPUTIME_ID, &ts);
#endif

    if (rados_initialized_) {
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s Reusing rados cluster connection", __func__);
        #endif
        return true;
    }

    rados_guard_.lock();
    if (rados_initialized_) {
        rados_guard_.unlock();
        #ifdef IRADOS_DEBUG
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s Reusing rados cluster connection", __func__);
        #endif
        return true;
    }

    uint64_t flags = 0;

    librados::Rados* cluster = new librados::Rados();
    int ret;
    /* Initialize the cluster handle with the "ceph" cluster name and "client.admin" user */
    {
        ret = cluster->init2(user_name, cluster_name, flags);
        if (ret < 0) {
            rodsLog(LOG_ERROR, "Couldn't initialize the cluster handle! error %d", ret);
            goto error;
        } else {
            #ifdef IRADOS_DEBUG
                rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s Created a new cluster handle", __func__);
            #endif
        }
    }

    /* Read a Ceph configuration file to configure the cluster handle. */
    {
        ret = cluster->conf_read_file("/etc/irods/irados.config");
        if (ret < 0) {
            rodsLog(LOG_ERROR, "Couldn't read the Ceph configuration file! error %d", ret);
            goto error;
        }
    }

    /* Connect to the cluster */
    {
        ret = cluster->connect();
        if (ret < 0) {
            rodsLog(LOG_ERROR, "Couldn't connect to the cluster! error %d", ret);
            goto error;
        } else {
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s Connected to the cluster.", __func__);
        }
    }

    rados_cluster_ = cluster;
    rados_initialized_ = true;
    
    rados_guard_.unlock();

#ifdef IRADOS_TIME
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    cout << "IRADOS_TIME: cluster_connect  " << ts.tv_sec << " " << ts.tv_nsec << endl;
#endif

    return true;

    error:
        rados_guard_.unlock();
        return false;
}


/**
 * Returns next free fd. Has to be encapsulated in a propmap_guard lock.
 */
int get_next_fd(irods::resource_plugin_context& _ctx) {
    int fd = 0;
    /*
     * During the plugin lifetime, multiple fds may be opened.
     * Simlply return a fresh one every time.
     */
    _ctx.prop_map().get < int > ("fd", fd);
    _ctx.prop_map().set< int >("fd", (fd + 1));
    return fd;
}


extern "C" {

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file registration
    irods::error irados_registered_plugin(
        irods::resource_plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        #endif

        return result;
    }

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file unregistration
    irods::error irados_unregistered_plugin(
        irods::resource_plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        #ifdef IRADOS_DEBUG
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s enter - %d", __func__, instance_id);
        #endif

        return result;
    }

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file modification
    irods::error irados_modified_plugin(
        irods::resource_plugin_context& _ctx ) {
        irods::error result = SUCCESS();

        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        #endif

        return result;
    }

    /// =-=-=-=-=-=-=-
    /// @brief interface to notify of a file operation
    irods::error irados_notify_plugin(
        irods::resource_plugin_context& _ctx,
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
        irods::resource_plugin_context& _ctx ) {
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



    rodsLong_t fssize = 1000000000;
    result.code( fssize );

    #ifdef IRADOS_DEBUG
        rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s leave", __func__);
    #endif
        return result;

    } // irados_get_fsfreespace_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX create
    irods::error irados_create_plugin(
        irods::resource_plugin_context& _ctx ) {

#ifdef IRADOS_TIME
        timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 0;
        clock_settime(CLOCK_PROCESS_CPUTIME_ID, &ts);
#endif

#ifdef IRADOS_DEBUG
//        rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s enter", __func__);
        // irods::stacktrace st;
        // st.trace();
        // st.dump();
#endif
        irods::error result = SUCCESS();

        // create the irados internal id for this data object
        std::string oid = rand_uuid_string();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        
        irods::error ret = irados_get_fsfreespace_plugin( _ctx );
        if ( ( result = ASSERT_PASS( ret, "Error determining freespace on system." ) ).ok() ) {
            rodsLong_t file_size = fop->size();
            if ( ( result = ASSERT_ERROR( file_size < 0 || ret.code() >= file_size, USER_FILE_TOO_LARGE, "File size: %ld is greater than space left on device: %ld", file_size, ret.code() ) ).ok() ) {
                fop->physical_path(oid);
            } else {
                result.code( PLUGIN_ERROR );
                return result;
            }
        }

        propmap_guard_.lock();
        
        int fd = get_next_fd(_ctx);
        fop->file_descriptor(fd);
                
        // creates and sets an initial seek ptr for the current fd.
        // _ctx.prop_map().set < uint64_t > ("OFFSET_PTR_" + fd, 0);
        fd_offsets_[fd] = 0;

        // This is the first!
        oids_open_fds_cnt_[oid] = 1;
        dirty_oids_[oid] = true;


        #ifdef IRADOS_DEBUG
            num_open_fds_++;
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s created: %s - (instance: %d, fd: %d) open fds: %d",
             __func__, oid.c_str(), instance_id, fd, num_open_fds_);
        #endif

        propmap_guard_.unlock();

        if (not connect_rados_cluster()) {
            rodsLog(LOG_ERROR, "irados: cannot connect to cluster.");
            result.code(PLUGIN_ERROR);
            return result;
        }

        assert(rados_cluster_ != NULL);

        librados::IoCtx* io_ctx;
        propmap_guard_.lock();
        irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);

        if (e.code() == KEY_NOT_FOUND) {
            propmap_guard_.unlock();
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s create creates new io_ctx.", __func__);
            if (not connect_rados_cluster()) {
                rodsLog( LOG_ERROR, "irados: cannot connect to cluster.");
                result.code(PLUGIN_ERROR);
                return result;
            }
            io_ctx = new librados::IoCtx();
            int ret = rados_cluster_->ioctx_create(pool_name, *io_ctx);
            if (ret < 0) {
                rodsLog( LOG_ERROR, "irados: cannot setup ioctx for cluster: error %d", ret);
                result.code(PLUGIN_ERROR);
                return result;
            }   
            propmap_guard_.lock();
            _ctx.prop_map().set<librados::IoCtx*>("CEPH_IOCTX", io_ctx); 
        } else {
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s open found existing io_ctx.", __func__);
        }
        propmap_guard_.unlock();

#ifdef IRADOS_TIME
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
        cout << "IRADOS_TIME: create  " << ts.tv_sec << " " << ts.tv_nsec << endl;
#endif
        return result;
    } // irados_create_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Open
    irods::error irados_open_plugin(
        irods::resource_plugin_context& _ctx ) {

// #ifdef IRADOS_DEBUG
//         irods::stacktrace st;
//         st.trace();
//         st.dump();
// #endif
//

        irods::error result = SUCCESS();
        
        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string oid = fop->physical_path();

        // test if the oid is a unique id. We have to get a uuid here.
        if (oid.find("/") != string::npos) {
            result.code(FILE_OPEN_ERR);
            return result;
        }

        librados::IoCtx* io_ctx;
        propmap_guard_.lock();
        irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);

        if (e.code() == KEY_NOT_FOUND) {
            propmap_guard_.unlock();

#ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s open creates new io_ctx.", __func__);
#endif
            if (not connect_rados_cluster()) {
                rodsLog(LOG_ERROR, "irados: cannot connect to cluster.");
                result.code(PLUGIN_ERROR);
                return result;
            }
            io_ctx = new librados::IoCtx();
            int ret = rados_cluster_->ioctx_create(pool_name, *io_ctx);
            if (ret < 0) {
                rodsLog(LOG_ERROR, "irados: cannot setup ioctx for cluster: error %d", ret);
                result.code(PLUGIN_ERROR);
                return result;
            }   
            propmap_guard_.lock();            
            _ctx.prop_map().set<librados::IoCtx*>("CEPH_IOCTX", io_ctx); 

        } else {
#ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s open found existing io_ctx.", __func__);
#endif
        }
        
        // gets the next free fd for this plugin instance
        int fd = get_next_fd(_ctx);
        fop->file_descriptor(fd);
        // creates and sets an initial seek ptr.
        // _ctx.prop_map().set < uint64_t > ("OFFSET_PTR_" + fd, 0);
        fd_offsets_[fd] = 0;

        // fd_cnt_for_this_object++
        oids_open_fds_cnt_[oid] += 1;
        dirty_oids_[oid] = false;

        #ifdef IRADOS_DEBUG
            num_open_fds_++;
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);
            std::string lid = fop->logical_path();
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s opened: oid: %s logical: %s - (instance: %d, fd: %d) open fds: %d | on file %d",
             __func__, oid.c_str(), lid.c_str(), instance_id, fd, num_open_fds_, oids_open_fds_cnt_[oid]);
        #endif
        
        propmap_guard_.unlock();
        return result;

    } // irados_open_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Read
    irods::error irados_read_plugin(
        irods::resource_plugin_context& _ctx,
        void* _buf,
        int _len) {
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        std::string oid = fop->physical_path();
        int fd = fop->file_descriptor();

        int status = 0;

        propmap_guard_.lock();
        // uint64_t read_ptr = 0;
        // _ctx.prop_map().get < uint64_t > ("OFFSET_PTR_" + fd, read_ptr);
        uint64_t read_ptr = fd_offsets_[fd];

        //Send read request.
        librados::IoCtx* io_ctx;
        irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);
        propmap_guard_.unlock();
        if (e.code() == KEY_NOT_FOUND) {
            // ioctx should have been created in open()
            result.code(PLUGIN_ERROR);
            return result;
        }

        uint64_t bytes_read = 0;
        
        while (bytes_read < _len) {
            uint64_t blob_id = (read_ptr + bytes_read) / RADOS_BLOB_SIZE;
            uint64_t blob_offset = (read_ptr + bytes_read) % RADOS_BLOB_SIZE;
            uint64_t read_len = std::min((_len - bytes_read), (RADOS_BLOB_SIZE - blob_offset));

            librados::bufferlist read_buf;

            // determine the correct object id of the block to write
            std::string blob_oid;
            std::stringstream out;
            if (blob_id > 0) {
                out << oid << "-" << blob_id;    
            } else {
                // the first block has no -XX identifiert
                out << oid;
            }
            blob_oid = out.str();

            status = io_ctx->read(blob_oid, read_buf, read_len, blob_offset);

            if (status < 0) {
                rodsLog( LOG_ERROR, "Couldn't read object '%s' - error: %d", blob_oid.c_str(), status);
                    result.code(PLUGIN_ERROR);
                    return result;
            }

            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s to %s blob_id: %d, blob_offset: %lu, write_len: %lu, rados_write_status: %d, (fd: %d)",
                    __func__, 
                    blob_oid.c_str(),
                    blob_id,
                    blob_offset,
                    read_len,
                    status,
                    fd);

            bytes_read += read_len;
            
            // read_buf.copy(status, (char*) _buf);
            // read_buf.copy(0, status, (char*) _buf);
            read_buf.copy(0, read_len, (char*) _buf + (_len - bytes_read));
        }
    
        #ifdef IRADOS_DEBUG
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);

            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s from %s off: %lu, len: %d RETURNED: %d - (instance: %d, fd: %d)",
                __func__, oid.c_str(), read_ptr, _len, status, instance_id, fd);
        #endif

        result.code(bytes_read);

        propmap_guard_.lock();
        // update the seek ptr.
        // _ctx.prop_map().set < uint64_t > ("OFFSET_PTR_" + fd, (read_ptr + status));
        fd_offsets_[fd] = read_ptr + status;
        propmap_guard_.unlock();

        return result;
    } // irados_read_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Write
    irods::error irados_write_plugin(
        irods::resource_plugin_context& _ctx,
        void* _buf,
        int _len) {

// #ifdef IRADOS_DEBUG
//         irods::stacktrace st;
//         st.trace();
//         st.dump();
// #endif

        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        std::string oid = fop->physical_path();
        int fd = fop->file_descriptor();

        librados::IoCtx* io_ctx;

        propmap_guard_.lock();
        irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);
        if (e.code() == KEY_NOT_FOUND) {
            propmap_guard_.unlock();
            // ioctx was created in open()
            rodsLog( LOG_ERROR, "IRADOS_DEBUG %s - %s - no IoCtx available", __func__, oid.c_str());
            result.code(PLUGIN_ERROR);
            return result;
        }

         // write_ptr = 0;
        // _ctx.prop_map().get < uint64_t > ("OFFSET_PTR_" + fd, write_ptr);
        uint64_t write_ptr = fd_offsets_[fd];

        propmap_guard_.unlock();

        // each irods file is split into multiple 4mb blobs that are stored in rados.
        // for the given offset + length, find the correct blob-id
        uint64_t bytes_written = 0;
        uint64_t blob_id = 0;

        while (bytes_written < _len) {
            blob_id = (write_ptr + bytes_written) / RADOS_BLOB_SIZE;
            uint64_t blob_offset = (write_ptr + bytes_written) % RADOS_BLOB_SIZE;
            
            uint64_t write_len = std::min((_len - bytes_written), (RADOS_BLOB_SIZE - blob_offset));

            librados::bufferlist write_buf;
            char* p = (char*)_buf;
            p += bytes_written;
            write_buf.append(p, write_len);

            // determine the correct object id of the block to write

            std::stringstream out;
            if (blob_id > 0) {
                out << oid << "-" << blob_id;    
            } else {
                // the first block has no -XX identifiert
                out << oid;
            }
            std::string blob_oid = out.str();

            int status = io_ctx->write(blob_oid, write_buf, write_len, blob_offset);
            
            if (status < 0) {
                rodsLog( LOG_ERROR, "Couldn't write object '%s' - error: %d", oid.c_str(), status);
                result.code(PLUGIN_ERROR);
                return result;
            }

            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s to %s blob_id: %d, blob_offset: %lu, write_len: %lu, rados_write_status: %d, (fd: %d)",
                    __func__, 
                    blob_oid.c_str(),
                    blob_id,
                    blob_offset,
                    write_len,
                    status,
                    fd);

            bytes_written += write_len;
        }


// #ifdef IRADOS_DEBUG
//         int instance_id = 0;
//         _ctx.prop_map().get < int> ("instance_id", instance_id);
//         rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s to %s off: %lu, len: %lu, rados_write_status: %d, (instance: %d, fd: %d)",
//             __func__, 
//             oid.c_str(),
//              write_ptr,
//             _len,
//             status,
//             instance_id,
//             fd);
// #endif

        propmap_guard_.lock();

        dirty_oids_[oid] = true;

        // _ctx.prop_map().set < uint64_t > ("OFFSET_PTR_" + fd, (write_ptr + _len));
        fd_offsets_[fd] = write_ptr + _len;

        // keep track of the highest offset for this file as it marks the actual file size.
        // finally, the file size will be written as the base objects xattr "object_size"
        uint64_t max_file_size = 0;
        _ctx.prop_map().get < uint64_t > ("SIZE_" + oid, max_file_size);
        if ((write_ptr + _len) > max_file_size) {
            _ctx.prop_map().set < uint64_t > ("SIZE_" + oid, (write_ptr + _len));            
        }

        // keep track of the number of total blobs of the file.
        // will be marked in the base objects xattr as well.
        uint64_t num_blobs = 0;
        _ctx.prop_map().get < uint64_t > ("NUM_BLOBS_" + oid, num_blobs);
        if (blob_id > num_blobs) {
            _ctx.prop_map().set < uint64_t > ("NUM_BLOBS_" + oid, blob_id);               
        }
        
        propmap_guard_.unlock();
        result.code( _len );
        return result;
    } // irados_write_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Close
    irods::error irados_close_plugin(
        irods::resource_plugin_context& _ctx ) {

        rodsLog( LOG_DEBUG, "IRADOS_DEBUG %s", __func__);
        irods::error result = SUCCESS();
        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        
        std::string oid = fop->physical_path();
        int fd = fop->file_descriptor();
        
        // TODO: maybe the ioctx needs to be closed / destroyed / deleted?
        
        propmap_guard_.lock();
        
        int fd_cnt = oids_open_fds_cnt_[oid];
        
        if (fd_cnt > 1) {
            // fd_cnt_for_this_object--
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s - %s - fd_cnt: %d SKIP UPDATE ON CLOSE", __func__, oid.c_str(), fd_cnt);
            oids_open_fds_cnt_[oid] -= 1;
            propmap_guard_.unlock();
        } else {
            // this was the last fd for the object.
            // _ctx.prop_map().erase("FD_CNT_" + oid);
            oids_open_fds_cnt_.erase(oid);

            // check if any of the opened fds actually changed something.
            bool dirty = dirty_oids_[oid];
            
            if (not dirty) {
                propmap_guard_.unlock();
            } else {
                rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s - %s - fd_cnt: %d Writing metadata for dirty oid", __func__, oid.c_str(), fd_cnt);
                uint64_t num_blobs = 0;
                _ctx.prop_map().get < uint64_t > ("NUM_BLOBS_" + oid, num_blobs);
                
                uint64_t max_file_size = 0;
                _ctx.prop_map().get < uint64_t > ("SIZE_" + oid, max_file_size);


                // this was the last fd for the file. now persist, num_blobs and file_size to the base oid object.
                librados::IoCtx* io_ctx;
                irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);
                propmap_guard_.unlock();

                if (e.code() == KEY_NOT_FOUND) {
                    // ioctx was created in open()
                    rodsLog( LOG_ERROR, "IRADOS_DEBUG %s - %s - no IoCtx available", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                    return result;
                }

                int r;           
                std::stringstream ss;

                librados::bufferlist xfilesize;
                ss << max_file_size;
                xfilesize.append(ss.str().c_str());
                r = io_ctx->setxattr(oid, "FILE_SIZE", xfilesize);
                if (r > 0) {
                    rodsLog( LOG_ERROR, "IRADOS_DEBUG %s - %s - error during IoCtx.setxattr()", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                }

                ss.str("");
                ss.clear();

                ss << num_blobs;
                librados::bufferlist xnumblobs;
                xnumblobs.append(ss.str().c_str());
                r = io_ctx->setxattr(oid, "NUM_BLOBS", xnumblobs);
                if (r > 0) {
                    rodsLog( LOG_ERROR, "IRADOS_DEBUG %s - %s - error during IoCtx.setxattr()", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                }

                ss.str("");
                ss.clear();

                ss << RADOS_BLOB_SIZE;
                librados::bufferlist xblobsize;
                xblobsize.append(ss.str().c_str());
                r = io_ctx->setxattr(oid, "BLOB_SIZE", xblobsize);
                if (r > 0) {
                    rodsLog( LOG_ERROR, "IRADOS_DEBUG %s - %s - error during IoCtx.setxattr()", __func__, oid.c_str());
                    result.code(PLUGIN_ERROR);
                }

    #ifdef IRADOS_DEBUG
                int instance_id = 0;
                _ctx.prop_map().get <int> ("instance_id", instance_id);  
                rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s %s - closed blob: highest blob-id: %lu, size: %lu (instance: %d, fd: %d)", __func__, oid.c_str(), num_blobs, max_file_size, instance_id, fd, num_open_fds_);
    #endif
            }   
        }


        return result;
    } // irados_close_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Unlink
    irods::error irados_unlink_plugin(
        irods::resource_plugin_context& _ctx ) {

#ifdef IRADOS_TIME
        timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 0;
        clock_settime(CLOCK_PROCESS_CPUTIME_ID, &ts);
#endif
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object>(_ctx.fco());
        std::string oid = fop->physical_path();

        librados::IoCtx* io_ctx;
        irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);

        if (e.code() == KEY_NOT_FOUND) {

            #ifdef IRADOS_DEBUG
                int instance_id = 0;
                _ctx.prop_map().get < int> ("instance_id", instance_id);

                rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s requires new IoCtx - %d", __func__, instance_id);
            #endif
     
            if (not connect_rados_cluster()) {
                rodsLog(LOG_ERROR, "irados: cannot connect to cluster.");
                result.code(PLUGIN_ERROR);
                return result;
            }
            
            io_ctx = new librados::IoCtx();
            int ret = rados_cluster_->ioctx_create(pool_name, *io_ctx);
            if (ret < 0) {
                std::cerr << "Couldn't set up ioctx! error " << ret
                        << std::endl;
                result.code(PLUGIN_ERROR);
                return result;
            }
            
            _ctx.prop_map().set<librados::IoCtx*>("CEPH_IOCTX", io_ctx);
        }

        // how many blobs do we have?
        librados::bufferlist bl;
        int r = io_ctx->getxattr(oid, "NUM_BLOBS", bl);
        
        if (r  < 0) {
            rodsLog( LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s io_ctx->getxattr returned: %d", __func__, oid.c_str(), r);
            result.code(PLUGIN_ERROR);
            return result;
        }

        uint64_t num_blobs = strtoul(bl.c_str(), NULL, 0);

        std::stringstream ss;
        std::string blob_oid;
        int status = 0;
        for (uint64_t blob_id = 0; blob_id <= num_blobs; blob_id++) {
            
            if (blob_id > 0) {
                ss << oid << "-" << blob_id;    
            } else {
                // the first block has no -XX identifiert
                ss << oid;
            }
            blob_oid = ss.str();
            ss.str("");
            ss.clear();
            
            status = io_ctx->remove(blob_oid);
            if (status != 0) {
                rodsLog( LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s io_ctx->remove(%s) returned: %d", __func__, oid.c_str(), blob_oid.c_str(), status);
            }
            result.code( status );
        }

        #ifdef IRADOS_DEBUG
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s %s removed %d blobs %d", __func__, oid.c_str(), num_blobs, instance_id);
        #endif

#ifdef IRADOS_TIME
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
        cout << "IRADOS_TIME: unlink  " << ts.tv_sec << " " << ts.tv_nsec << endl;
#endif
        return result;
    } // irados_unlink_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX Stat
    irods::error irados_stat_plugin(
        irods::resource_plugin_context& _ctx,
        struct stat* _statbuf ) {
        irods::error result = SUCCESS();

#ifdef IRADOS_TIME
        timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 0;
        clock_settime(CLOCK_PROCESS_CPUTIME_ID, &ts);
#endif

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        std::string oid = fop->physical_path();

        #ifdef IRADOS_DEBUG
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s -> oid: %s", __func__, oid.c_str());
        #endif

        librados::IoCtx* io_ctx;

        irods::error e = _ctx.prop_map().get<librados::IoCtx*>("CEPH_IOCTX", io_ctx);
        if (e.code() == KEY_NOT_FOUND) {
            result.code(PLUGIN_ERROR);
            return result;
        }

        uint64_t psize;
        time_t pmtime;

        int status = io_ctx->stat(oid, &psize, &pmtime);

        librados::bufferlist xfilesize;
        int r = io_ctx->getxattr(oid, "FILE_SIZE", xfilesize);

        if (r != 0) {
            rodsLog( LOG_ERROR, "IRADOS_DEBUG %s -> oid: %s io_ctx->getxattr returned: %d", __func__, oid.c_str(), r);
        }

        uint64_t file_size = strtoul(xfilesize.c_str(), NULL, 0);

        _statbuf->st_mtime = pmtime;
        _statbuf->st_size = file_size;

         #ifdef IRADOS_DEBUG
            _ctx.prop_map().get < int > ("instance_id", instance_id);
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s -> status: %d, oid: %s stat got pmtime: %lu, size:%lu - %d", __func__, status, oid.c_str(), pmtime, file_size, instance_id);
        #endif

        result.code(status);
#ifdef IRADOS_TIME
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
        cout << "IRADOS_TIME: stat " << ts.tv_sec << " " << ts.tv_nsec << endl;
#endif 

        return result;
    } // irados_stat_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX lseek
    irods::error irados_lseek_plugin(
        irods::resource_plugin_context& _ctx,
        long long                           _offset,
        int                                 _whence ) { // always is SEEK_SET == 0
        irods::error result = SUCCESS();

        irods::file_object_ptr fop = boost::dynamic_pointer_cast< irods::file_object >( _ctx.fco() );
        int fd = fop->file_descriptor();

        // _ctx.prop_map().set< uint64_t>(("OFFSET_PTR_" + fd), _offset);
        fd_offsets_[fd] = _offset;
        result.code(_offset);
        
        #ifdef IRADOS_DEBUG
            int instance_id = 0;
            _ctx.prop_map().get < int> ("instance_id", instance_id);
            rodsLog(LOG_NOTICE, "IRADOS_DEBUG %s offset: %lu, whence: %d - (instance: %d, fd: %d)", __func__, _offset, _whence, instance_id, fd);
        #endif

        return result;
    } // irados_lseek_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX mkdir
    irods::error irados_mkdir_plugin(
        irods::resource_plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif

        // we do not know any directory.
        return SUCCESS();
    } // irados_mkdir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX rmdir
    irods::error irados_rmdir_plugin(
        irods::resource_plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif
        // we do not know any directory.
        return SUCCESS();
    } // irados_rmdir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX opendir
    irods::error irados_opendir_plugin(
        irods::resource_plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
         return ERROR( SYS_NOT_SUPPORTED, "irados_opendir_plugin" );
    } // irados_opendir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX closedir
    irods::error irados_closedir_plugin(
        irods::resource_plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
        return ERROR( SYS_NOT_SUPPORTED, "irados_closedir_plugin" );
    } // irados_closedir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error irados_readdir_plugin(
        irods::resource_plugin_context& _ctx,
        struct rodsDirent** _dirent_ptr ) {
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
        return ERROR( SYS_NOT_SUPPORTED, "irados_readdir_plugin" );
    } // irados_readdir_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX readdir
    irods::error irados_rename_plugin(
        irods::resource_plugin_context& _ctx,
        const char* _new_file_name ) {
        
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s", __func__);
        #endif
        
        // rename in rados is a: get old_oid, put old_oid new_oid, rm old_oid
        // we have a fixed physical path and do not reflect changes to the physical path.
        return SUCCESS();
    } // irados_rename_plugin

    // =-=-=-=-=-=-=-
    // interface for POSIX truncate
    irods::error irados_truncate_plugin(
        irods::resource_plugin_context& _ctx ) {
        #ifdef IRADOS_DEBUG
            rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s - not supported", __func__);
        #endif
        return ERROR( SYS_NOT_SUPPORTED, "irados_truncate_plugin" );
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
    rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s leave", __func__);
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
    rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s leave", __func__);
#endif
        return result;

    } // irados_redirect_open


     // =-=-=-=-=-=-=-
     // used to allow the resource to determine which host
     // should provide the requested operation
     irods::error irados_resolve_hierarchy_plugin(
         irods::resource_plugin_context& _ctx,
         const std::string* _opr,
         const std::string* _curr_host,
         irods::hierarchy_parser* _out_parser,
         float* _out_vote ) {
         irods::error result = SUCCESS();

         rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s called %s -> %s", __func__, _opr->c_str(), _curr_host->c_str());


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
                             irods::WRITE_OPERATION == ( *_opr ) ) {
                         // =-=-=-=-=-=-=-
                         // call redirect determination for 'get' operation
                         ret = irados_redirect_open( _ctx.prop_map(), file_obj, resc_name, ( *_curr_host ), ( *_out_vote ) );
                         result = ASSERT_PASS_MSG( ret, "Failed redirecting for open." );

                     }
                     else if ( irods::CREATE_OPERATION == ( *_opr ) ) {
                         // =-=-=-=-=-=-=-
                         // call redirect determination for 'create' operation
                         ret = irados_redirect_create( _ctx.prop_map(), file_obj, resc_name, ( *_curr_host ), ( *_out_vote ) );
                         result = ASSERT_PASS_MSG( ret, "Failed redirecting for create." );
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
         irods::resource_plugin_context& _ctx ) {
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
    rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s CTOR (%s, %s)", __func__, _inst_name.c_str(), _context.c_str());
#endif
        } // ctor


//        // =-=-=-=-=-=-=-
//        // 3b. pass along a functor for maintenance work after
//        //     the client disconnects, uncomment the first two lines for effect.
        irods::error post_disconnect_maintenance_operation( irods::pdmo_type& _op ) {
            irods::error result = SUCCESS();

            #ifdef IRADOS_DEBUG
                rodsLog( LOG_NOTICE, "IRADOS_DEBUG %s POST DISCONNECT HOOK", __func__);
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
irods::resource* plugin_factory(const std::string& _inst_name,
        const std::string& _context) {
    
    // =-=-=-=-=-=-=-
    // 4a. create irados_resource
    irados_resource* resc = new irados_resource(_inst_name, _context);

    // =-=-=-=-=-=-=-
    // 4b. map function names to operations.  this map will be used to load
    //     the symbols from the shared object in the delay_load stage of
    //     plugin loading.
    resc->add_operation(irods::RESOURCE_OP_CREATE, "irados_create_plugin");
    resc->add_operation(irods::RESOURCE_OP_OPEN, "irados_open_plugin");
    resc->add_operation(irods::RESOURCE_OP_READ, "irados_read_plugin");
    resc->add_operation(irods::RESOURCE_OP_WRITE, "irados_write_plugin");
    resc->add_operation(irods::RESOURCE_OP_CLOSE, "irados_close_plugin");
    resc->add_operation(irods::RESOURCE_OP_UNLINK, "irados_unlink_plugin");
    resc->add_operation(irods::RESOURCE_OP_STAT, "irados_stat_plugin");
    resc->add_operation(irods::RESOURCE_OP_LSEEK, "irados_lseek_plugin");
    resc->add_operation(irods::RESOURCE_OP_MKDIR, "irados_mkdir_plugin");
    resc->add_operation(irods::RESOURCE_OP_RMDIR, "irados_rmdir_plugin");
    resc->add_operation(irods::RESOURCE_OP_OPENDIR, "irados_opendir_plugin");
    resc->add_operation(irods::RESOURCE_OP_CLOSEDIR, "irados_closedir_plugin");
    resc->add_operation(irods::RESOURCE_OP_READDIR, "irados_readdir_plugin");
    resc->add_operation(irods::RESOURCE_OP_RENAME, "irados_rename_plugin");
    resc->add_operation(irods::RESOURCE_OP_TRUNCATE, "irados_truncate_plugin");
    resc->add_operation(irods::RESOURCE_OP_FREESPACE, "irados_get_fsfreespace_plugin");

    resc->add_operation( irods::RESOURCE_OP_REGISTERED,   "irados_registered_plugin" );
    resc->add_operation( irods::RESOURCE_OP_UNREGISTERED, "irados_unregistered_plugin" );
    resc->add_operation( irods::RESOURCE_OP_MODIFIED,     "irados_modified_plugin" );
    resc->add_operation( irods::RESOURCE_OP_NOTIFY,       "irados_notify_plugin" );

    resc->add_operation(irods::RESOURCE_OP_RESOLVE_RESC_HIER, "irados_resolve_hierarchy_plugin");
    resc->add_operation(irods::RESOURCE_OP_REBALANCE, "irados_rebalance");

    // =-=-=-=-=-=-=-
    // set some properties necessary for backporting to iRODS legacy code
    resc->set_property<int>(irods::RESOURCE_CHECK_PATH_PERM, 2); //DO_CHK_PATH_PERM );
    resc->set_property<int>(irods::RESOURCE_CREATE_PATH, 1);    //CREATE_PATH );

    #ifdef IRADOS_DEBUG
        srand(time(NULL)); 
        int instance_id = rand() % 100000;
        resc->set_property<int>("instance_id", instance_id);
        rodsLog( LOG_NOTICE, "IRADOS_DEBUG Plugin created with instance_id: %d", instance_id);
    #endif

    return dynamic_cast<irods::resource*>(resc);

} // plugin_factory

}; // extern "C"
