#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

extern "C" {
    #include <fcntl.h>
    #include <unistd.h>

    #include "RDXJ.h"
    #include "RDXY.h"
    #include "abc/$.h"
    #include "abc/FILE.h"
    #include "abc/INT.h"
}


u8 *output[4] = {};
u8 *input[4] = {};
u8 *mergeBuf[4] = {};
$u8c *ins[4] = {};

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

std::string mergeRDX(const std::string& existing_value, const std::vector<std::string>& operands) {
    char* result;

    if (Bu8map(output, 1UL << 32) || 
        Bu8map(input, 1UL << 32) || 
        Bu8map(mergeBuf, 1UL << 32) || 
        B$u8cmap(ins, RDXY_MAX_INPUTS)) {
        handleError("Failed to allocate memory for buffers.");
    }

    try {
        // Записываем существующее значение в буфер
        u8 **into = Bu8idle(output);
        strncpy((char*)*into, existing_value.c_str(), existing_value.length());
        *into += existing_value.length();

        // Преобразуем строку в формат TLV
        RDXJdrain(Bu8idle(input), Bu8cdata(output));
        Breset(output);
        TLVsplit(B$u8cidle(ins), Bu8cdata(input));

        // Обрабатываем все операнды
        for (const auto& operand : operands) {
            strncpy((char*)*into, operand.c_str(), operand.length());
            *into += operand.length();

            RDXJdrain(Bu8idle(input), Bu8cdata(output));
            Breset(output);
            TLVsplit(B$u8cidle(ins), Bu8cdata(input));
        }

        // Выполняем слияние
        RDXY(Bu8idle(mergeBuf), B$u8cdata(ins));
        B$u8cunmap(ins);
        B$u8cmap(ins, RDXY_MAX_INPUTS);
        TLVsplit(B$u8cidle(ins), Bu8cdata(mergeBuf));

        // Преобразуем результат обратно в формат JDR
        a$dup($u8c, in, B$u8cdata(ins));
        RDXJfeed(Bu8idle(output), **in);
        ++*in;
        $eat(in) {
            $u8feed2(Bu8idle(output), ',', '\n');
            RDXJfeed(Bu8idle(output), **in);
        }

        // Преобразуем JDR результат в std::string
        size_t resultLength = $len(Bu8cdata(output));
        result = (char*)malloc(resultLength + 1);
        strncpy(result, (char*)*Bu8cdata(output), resultLength);
        result[resultLength] = '\0';

        std::string final_result(result);

        // Очистка ресурсов
        free(result);
        Bu8unmap(output);
        Bu8unmap(input);
        Bu8unmap(mergeBuf);
        B$u8cunmap(ins);

        return final_result;
    } catch (...) {
        // Очистка ресурсов
        free(result);
        Bu8unmap(output);
        Bu8unmap(input);
        Bu8unmap(mergeBuf);
        B$u8cunmap(ins);
        handleError("An unexpected error occurred.");
    }

    return "";
}

class MyMerge : public ROCKSDB_NAMESPACE::MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    // Обрабатываем существующее значение и операнды
    std::vector<std::string> operands;
    for (const auto& operand : merge_in.operand_list) {
        operands.push_back(operand.ToString());
    }
    merge_out->new_value = mergeRDX(
        merge_in.existing_value ? merge_in.existing_value->ToString() : "{}",
        operands
    );
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
