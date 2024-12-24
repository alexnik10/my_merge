// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include <iostream>
#include <sstream>
#include <memory>

class MyMerge : public ROCKSDB_NAMESPACE::MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    merge_out->new_value.clear();
    if (merge_in.existing_value != nullptr) {
      merge_out->new_value.assign(merge_in.existing_value->data(),
                                  merge_in.existing_value->size());
    }
    for (const ROCKSDB_NAMESPACE::Slice& m : merge_in.operand_list) {
      merge_out->new_value.assign(m.data(), m.size());
    }
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

int main() {
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
