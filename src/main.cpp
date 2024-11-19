#include <iostream>
extern "C" {
#include "RDXJ.h"
#include "RDXY.h"
}

void TestRDXMerge() {
  std::cout << "Testing RDX Merge...\n";

  const char* input1 = R"({"key1":"value1"})";
  const char* input2 = R"({"key2":"value2"})";

  u8* input[4] = {};
  u8* output = nullptr;

  Bu8map(output, 1UL << 32);
  Bu8map(input, 1UL << 32);

  try {
    $u8cfeed1(Bu8idle(input), reinterpret_cast<const u8*>(input1), strlen(input1));
    $u8cfeed1(Bu8idle(input), reinterpret_cast<const u8*>(input2), strlen(input2));

    RDXY(Bu8idle(output), B$u8cdata(input));

    std::string result(reinterpret_cast<const char*>(Bu8cdata(output)), Bsize(output));
    std::cout << "Merge Result: " << result << "\n";

  } catch (...) {
    std::cerr << "Error occurred during RDX merge!\n";
  }

  Bu8unmap(input);
  Bu8unmap(output);

  std::cout << "RDX Merge Test Completed.\n";
}


int main() {
  TestRDXMerge();
  return 0;
}

