#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <vector>

extern "C" {
    #include <fcntl.h>
    #include <unistd.h>

    #include "RDXJ.h"
    #include "RDXY.h"
    #include "abc/$.h"
    #include "abc/FILE.h"
    #include "abc/INT.h"
}

ABC_INIT

class RdxMerger {
private:
    u8 *buf1[4] = {};
    u8 *buf2[4] = {};
    $u8c *ins[4] = {};

    void initializeBuffers() {
        Bu8map(buf1, 1UL << 32);
        Bu8map(buf2, 1UL << 32);
        B$u8cmap(ins, RDXY_MAX_INPUTS);
    }

    void cleanupBuffers() {
        Bu8unmap(buf1);
        Bu8unmap(buf2);
        B$u8cunmap(ins);
    }

    void convertSliceTo$u8(const rocksdb::Slice& s, $u8c into) {
        into[0] = (u8*)s.data();
        into[1] = into[0] + s.size();
    }

    std::string convert$u8ToStr($cu8c data) {
        return std::string((char*)$head(data), $len(data));
    }

    ok64 splitTLV($$u8c idle, $cu8c data) {
        sane($ok(idle) && $ok(data));
        a$dup(u8c, d, data);
        while (!$empty(d)) {
            $u8c next = {};
            call(TLVdrain$, next, d);
            call($$u8cfeed1, idle, next);
        }
        done;
    }

public:
    RdxMerger() {
        initializeBuffers();
    }

    ~RdxMerger() {
        cleanupBuffers();
    }

    std::string merge(const std::vector<rocksdb::Slice>& operands) {
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
};


class RdxMergeOperator : public rocksdb::MergeOperator {
 public:
   mutable RdxMerger merger;

    RdxMergeOperator() : merger() {}

bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    std::vector<rocksdb::Slice> operands(merge_in.operand_list);
    if (merge_in.existing_value){
        operands.push_back(*merge_in.existing_value);
    }
    try {
        merge_out->new_value = merger.merge(operands);
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Merge error: " << e.what() << std::endl;
        return false;
    }
  }

  const char* Name() const override { return "RdxMergeOperator"; }
};

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksmergetest";
std::string kRemoveDirCommand = "rmdir /Q /S ";
#else
std::string kDBPath = "/tmp/rocksmergetest";
std::string kRemoveDirCommand = "rm -rf ";
#endif

void deleteDBData() {
    std::string rm_cmd = kRemoveDirCommand + kDBPath;
    int ret = system(rm_cmd.c_str());
    if (ret != 0) {
        std::cerr << "Error deleting " << kDBPath << ", code: " << ret << std::endl;
    }
}

rocksdb::Status openRocksDB(rocksdb::DB*& db){
    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new RdxMergeOperator);

    return rocksdb::DB::Open(options, kDBPath, &db);
}

void handleMergeCommand(rocksdb::DB& db, const std::string& key, const std::string& value) {
    auto status = db.Merge(rocksdb::WriteOptions(), key, value);

    if (!status.ok()) {
        std::cerr << "Merge failed: " << status.ToString() << std::endl;
    } else {
        std::cout << "Merged key: " << key << " with value: " << value << std::endl;
    }
}

void handleGetCommand(rocksdb::DB& db, const std::string& key) {
    std::string result;
    auto status = db.Get(rocksdb::ReadOptions(), key, &result);

    if (status.IsNotFound()) {
        std::cout << "Key not found: " << key << std::endl;
    } else if (!status.ok()) {
        std::cerr << "Get failed: " << status.ToString() << std::endl;
    } else {
        std::cout << "Key: " << key << ", Value: " << result << std::endl;
    }
}

void processCommands(rocksdb::DB& db){
    std::string command;
    std::cout << "Enter commands: merge <key> <value> or get <key> (type 'exit' to quit)" << std::endl;

    while (true) {
        std::cout << "> ";
        std::getline(std::cin, command);
        if (command == "exit") {
            break;
        }

        std::istringstream iss(command);
        std::string cmd, key, value;
        iss >> cmd;

        if (cmd == "merge") {
            iss >> key >> value;
            if (!key.empty() && !value.empty()) {
                handleMergeCommand(db, key, value);
            } else {
                std::cerr << "Invalid merge command. Usage: merge <key> <value>" << std::endl;
            }
        } else if (cmd == "get") {
            iss >> key;
            if (!key.empty()) {
                handleGetCommand(db, key);
            } else {
                std::cerr << "Invalid get command. Usage: get <key>" << std::endl;
            }
        } else {
            std::cerr << "Unknown command. Supported commands: merge, get, exit" << std::endl;
        }
    }
}

int my_merge_main() {
    deleteDBData();

    rocksdb::DB* db;
    auto status = openRocksDB(db);

    if (!status.ok()) {
        std::cerr << "Error opening database: " << status.ToString() << std::endl;
        return 1;
    }

    processCommands(*db);

    delete db;
    
    return 0;
}

int main() {
    int ret = my_merge_main();
    return ret;
}
