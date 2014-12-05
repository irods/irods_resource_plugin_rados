#ifndef _LIBIRADOS_H_
#define _LIBIRADOS_H_

// RADOS
#include <rados/librados.hpp>

struct rados_conn_t {
    librados::Rados* cluster_;
    librados::IoCtx* io_ctx_;
};

#endif