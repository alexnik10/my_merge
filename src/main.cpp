#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include <sstream>
#include "RdxMerger.h"
#include "RdxMergeOperator.h"

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
