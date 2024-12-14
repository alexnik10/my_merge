#include <iostream>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

extern "C" {
    #include "RDXJ.h"
    #include "RDXY.h"
    #include "abc/$.h"
    #include "abc/FILE.h"
    #include "abc/INT.h"
}

void handleError(const char* message) {
    std::cerr << "Error: " << message << std::endl;
    exit(EXIT_FAILURE);
}

int my_merge_main() {
    // Исходные строки в формате RDX-JDR
    const char* input1 = "<1:2>,<1:1:3>";
    const char* input2 = "{3:4,4:5,\"seven\"}";

    u8 *output[4] = {};
    u8 *input1Buf[4] = {};
    u8 *input2Buf[4] = {};
    $u8c *ins[4] = {};

    // Инициализация буферов
    if (Bu8map(output, 1UL << 32) || 
        Bu8map(input1Buf, 1UL << 32) || 
        Bu8map(input2Buf, 1UL << 32) || 
        B$u8cmap(ins, RDXY_MAX_INPUTS)) {
        handleError("Failed to allocate memory for buffers.");
    }

    try {
        // Записываем строки в буферы
        u8 **into = Bu8idle(input1Buf);
        strncpy((char*)*into, input1, strlen(input1));
        *into += strlen(input1);

        // Преобразуем строки в формат TLV

        if (RDXJdrain(Bu8idle(output), Bu8cdata(input1Buf))) {
            handleError("Failed to convert to TLV format.");
        }

        // // Добавляем TLV-данные в массив `ins`
        // TLVsplit(B$u8cidle(&ins), Bu8cdata(&input1Buffer));
        // TLVsplit(B$u8cidle(&ins), Bu8cdata(&input2Buffer));

        // // Выполняем слияние
        // if (!RDXY(Bu8idle(&output), B$u8cdata(&ins))) {
        //     handleError("Failed to merge data.");
        // }

        // // Преобразуем результат обратно в формат JDR
        // if (!RDXJfeed(Bu8idle(&output), Bu8cdata(&output))) {
        //     handleError("Failed to convert to JDR format.");
        // }

        // Вывод результата на консоль
        // FILEfeedall(STDOUT_FILENO, Bu8cdata(output));
        std::cout << "finish" << std::endl;
    } catch (...) {
        handleError("An unexpected error occurred.");
    }

    // Очистка ресурсов
    Bu8unmap(output);
    Bu8unmap(input1Buf);
    Bu8unmap(input2Buf);
    B$u8cunmap(ins);

    return 0;
}

MAIN(my_merge_main)
