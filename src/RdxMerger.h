#ifndef RDXMERGER_H
#define RDXMERGER_H

#include "rocksdb/slice.h"
#include <string>
#include <vector>
#include <stdexcept>

extern "C" {
    #include "RDXJ.h"
    #include "RDXY.h"
    #include "abc/FILE.h"
}

class RdxMerger {
private:
    u8 *buf1[4] = {};
    u8 *buf2[4] = {};
    $u8c *ins[4] = {};

    void initializeBuffers();
    void cleanupBuffers();
    void convertSliceTo$u8(const rocksdb::Slice& s, $u8c into);
    std::string convert$u8ToStr($cu8c data);
    ok64 splitTLV($$u8c idle, $cu8c data);

public:
    RdxMerger();
    ~RdxMerger();
    std::string merge(const std::vector<rocksdb::Slice>& operands);
};

#endif // RDXMERGER_H
