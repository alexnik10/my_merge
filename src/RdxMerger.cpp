#include "RdxMerger.h"

ABC_INIT

void RdxMerger::initializeBuffers() {
        Bu8map(buf1, 1UL << 32);
        Bu8map(buf2, 1UL << 32);
        B$u8cmap(ins, RDXY_MAX_INPUTS);
    }

void RdxMerger::cleanupBuffers() {
    Bu8unmap(buf1);
    Bu8unmap(buf2);
    B$u8cunmap(ins);
}

void RdxMerger::convertSliceTo$u8(const rocksdb::Slice& s, $u8c into) {
    into[0] = (u8*)s.data();
    into[1] = into[0] + s.size();
}

std::string RdxMerger::convert$u8ToStr($cu8c data) {
    return std::string((char*)$head(data), $len(data));
}

ok64 RdxMerger::splitTLV($$u8c idle, $cu8c data) {
    sane($ok(idle) && $ok(data));
    a$dup(u8c, d, data);
    while (!$empty(d)) {
        $u8c next = {};
        call(TLVdrain$, next, d);
        call($$u8cfeed1, idle, next);
    }
    done;
}

RdxMerger::RdxMerger() {
    initializeBuffers();
}

RdxMerger::~RdxMerger() {
    cleanupBuffers();
}

std::string RdxMerger::merge(const std::vector<rocksdb::Slice>& operands) {
    try {
        $u8c jdrVal;

        for (const auto& operand : operands) {
            convertSliceTo$u8(operand, jdrVal);
            RDXJdrain(Bu8idle(buf1), jdrVal);
        }

        splitTLV(B$u8cidle(ins), Bu8cdata(buf1));

        RDXY(Bu8idle(buf2), B$u8cdata(ins));

        Breset(buf1);
        Breset(ins);

        splitTLV(B$u8cidle(ins), Bu8cdata(buf2));

        a$dup($u8c, in, B$u8cdata(ins));
        RDXJfeed(Bu8idle(buf1), **in);
        ++*in;
        $eat(in) {
            $u8feed2(Bu8idle(buf1), ',', '\n');
            RDXJfeed(Bu8idle(buf1), **in);
        }

        std::string merge_result = convert$u8ToStr(Bu8cdata(buf1));

        Breset(buf1);
        Breset(buf2);
        Breset(ins);

        return merge_result;
    } catch (...) {
        throw std::runtime_error("An unexpected error occurred during merging.");
    }
}
