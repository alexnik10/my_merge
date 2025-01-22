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


class RDXBufferManager {
public:
    u8 *buf1[4] = {};
    u8 *buf2[4] = {};
    $u8c *ins[4] = {};

    RDXBufferManager() = default;

    void initializeBuffers() {
        if (!buf1[0] && Bu8map(buf1, 1UL << 32)) {
            throw std::runtime_error("Failed to allocate buf1.");
        }
        if (!buf2[0] && Bu8map(buf2, 1UL << 32)) {
            throw std::runtime_error("Failed to allocate buf2.");
        }
        if (!ins[0] && B$u8cmap(ins, RDXY_MAX_INPUTS)) {
            throw std::runtime_error("Failed to allocate ins.");
        }
    }

    ~RDXBufferManager() {
        Bu8unmap(buf1);
        Bu8unmap(buf2);
        B$u8cunmap(ins);
    }
};

fun pro(TLVsplit, $$u8c idle, $cu8c data) {
    sane($ok(idle) && $ok(data));
    a$dup(u8c, d, data);
    while (!$empty(d)) {
        $u8c next = {};
        call(TLVdrain$, next, d);
        call($$u8cfeed1, idle, next);
    }
    done;
}

void handleError(const char* message) {
    std::cerr << "Error: " << message << std::endl;
    exit(EXIT_FAILURE);
}

void convertSliceTo$u8(const rocksdb::Slice& s, $u8c into){
    into[0] = (u8*)s.data();
    into[1] = into[0] + s.size();
}

std::string convert$u8ToStr($cu8c data){
    return std::string((char*)$head(data), $len(data));
}

std::string mergeRDX(const std::vector<rocksdb::Slice>& operands, RDXBufferManager& buffers) {
    buffers.initializeBuffers();

    try {
        $u8c jdrVal;

        // Обрабатываем все операнды
        // Преобразуем строку в формат TLV и записываем в буфер
        for (const auto& operand : operands) {
            convertSliceTo$u8(operand, jdrVal);
            RDXJdrain(Bu8idle(buffers.buf1), jdrVal);
        }

        TLVsplit(B$u8cidle(buffers.ins), Bu8cdata(buffers.buf1));

        // Выполняем слияние
        RDXY(Bu8idle(buffers.buf2), B$u8cdata(buffers.ins));

        Breset(buffers.buf1);
        Breset(buffers.ins);

        TLVsplit(B$u8cidle(buffers.ins), Bu8cdata(buffers.buf2));

        // Преобразуем результат обратно в формат JDR
        a$dup($u8c, in, B$u8cdata(buffers.ins));
        RDXJfeed(Bu8idle(buffers.buf1), **in);
        ++*in;
        $eat(in) {
            $u8feed2(Bu8idle(buffers.buf1), ',', '\n');
            RDXJfeed(Bu8idle(buffers.buf1), **in);
        }

        // Преобразуем JDR результат в std::string
        std::string merge_result = convert$u8ToStr(Bu8cdata(buffers.buf1));

        Breset(buffers.buf1);
        Breset(buffers.buf2);
        Breset(buffers.ins);

        return merge_result;
    } catch (...) {
        handleError("An unexpected error occurred.");
    }

    return "";
}

class MyMerge : public rocksdb::MergeOperator {
 public:
   mutable RDXBufferManager buffers;

  MyMerge() : buffers() {
    buffers = RDXBufferManager();
  }

bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    // Обрабатываем существующее значение и операнды
    std::vector<rocksdb::Slice> operands(merge_in.operand_list);
    if (merge_in.existing_value){
        operands.push_back(*merge_in.existing_value);
    }
    merge_out->new_value = mergeRDX(operands, buffers);
    return true;
  }

  const char* Name() const override { return "MyMerge"; }
};

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksmergetest";
std::string kRemoveDirCommand = "rmdir /Q /S ";
#else
std::string kDBPath = "/tmp/rocksmergetest";
std::string kRemoveDirCommand = "rm -rf ";
#endif

int my_merge_main() {
    rocksdb::DB* raw_db;
    rocksdb::Status status;

    std::string rm_cmd = kRemoveDirCommand + kDBPath;
    int ret = system(rm_cmd.c_str());
    if (ret != 0) {
        std::cerr << "Error deleting " << kDBPath << ", code: " << ret << std::endl;
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new MyMerge);
    status = rocksdb::DB::Open(options, kDBPath, &raw_db);
    if (!status.ok()) {
        std::cerr << "Error opening database: " << status.ToString() << std::endl;
        return 1;
    }
    std::unique_ptr<rocksdb::DB> db(raw_db);

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
                rocksdb::WriteOptions wopts;
                status = db->Merge(wopts, key, value);
                if (!status.ok()) {
                    std::cerr << "Merge failed: " << status.ToString() << std::endl;
                } else {
                    std::cout << "Merged key: " << key << " with value: " << value << std::endl;
                }
            } else {
                std::cerr << "Invalid merge command. Usage: merge <key> <value>" << std::endl;
            }
        } else if (cmd == "get") {
            iss >> key;
            if (!key.empty()) {
                std::string result;
                status = db->Get(rocksdb::ReadOptions(), key, &result);
                if (status.IsNotFound()) {
                    std::cout << "Key not found: " << key << std::endl;
                } else if (!status.ok()) {
                    std::cerr << "Get failed: " << status.ToString() << std::endl;
                } else {
                    std::cout << "Key: " << key << ", Value: " << result << std::endl;
                }
            } else {
                std::cerr << "Invalid get command. Usage: get <key>" << std::endl;
            }
        } else {
            std::cerr << "Unknown command. Supported commands: merge, get, exit" << std::endl;
        }
    }

    return 0;
}

MAIN(my_merge_main)
