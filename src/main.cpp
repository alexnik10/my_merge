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

void convertStrTo$(const std::string& s, $u8c into){
    into[0] = (u8*)s.c_str();
    into[1] = into[0] + s.length();
}

std::string mergeRDX(const std::vector<std::string>& operands, RDXBufferManager& buffers) {
    char* result;

    buffers.initializeBuffers();

    try {
        $u8c jdrVal;

        // Обрабатываем все операнды
        // Преобразуем строку в формат TLV и записываем в буфер
        for (const auto& operand : operands) {
            convertStrTo$(operand, jdrVal);
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
        size_t resultLength = $len(Bu8cdata(buffers.buf1));
        result = (char*)malloc(resultLength + 1);
        strncpy(result, (char*)*Bu8cdata(buffers.buf1), resultLength);
        result[resultLength] = '\0';

        std::string final_result(result);

        free(result);
        Breset(buffers.buf1);
        Breset(buffers.buf2);
        Breset(buffers.ins);

        return final_result;
    } catch (...) {
        free(result);
        handleError("An unexpected error occurred.");
    }

    return "";
}

class MyMerge : public ROCKSDB_NAMESPACE::MergeOperator {
 public:
   mutable RDXBufferManager buffers;

  MyMerge() : buffers() {
    buffers = RDXBufferManager();
  }

bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    // Обрабатываем существующее значение и операнды
    std::vector<std::string> operands;
    if (merge_in.existing_value){
        operands.push_back(merge_in.existing_value->ToString());
    }
    for (const auto& operand : merge_in.operand_list) {
        operands.push_back(operand.ToString());
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
    ROCKSDB_NAMESPACE::DB* raw_db;
    ROCKSDB_NAMESPACE::Status status;

    std::string rm_cmd = kRemoveDirCommand + kDBPath;
    int ret = system(rm_cmd.c_str());
    if (ret != 0) {
        std::cerr << "Error deleting " << kDBPath << ", code: " << ret << std::endl;
    }

    ROCKSDB_NAMESPACE::Options options;
    options.create_if_missing = true;
    options.merge_operator.reset(new MyMerge);
    status = ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &raw_db);
    if (!status.ok()) {
        std::cerr << "Error opening database: " << status.ToString() << std::endl;
        return 1;
    }
    std::unique_ptr<ROCKSDB_NAMESPACE::DB> db(raw_db);

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
                ROCKSDB_NAMESPACE::WriteOptions wopts;
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
                status = db->Get(ROCKSDB_NAMESPACE::ReadOptions(), key, &result);
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
