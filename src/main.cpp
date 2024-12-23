#include <iostream>
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

int my_merge_main() {
    // Исходные строки в формате RDX-JDR
    const char* input1 = "<1:2>,<1:1:3>";
    const char* input2 = "{3:4,4:5,\"seven\"}";

    char* result;

    // Инициализация буферов
    if (Bu8map(output, 1UL << 32) || 
        Bu8map(input, 1UL << 32) || 
        Bu8map(mergeBuf, 1UL << 32) || 
        B$u8cmap(ins, RDXY_MAX_INPUTS)) {
        handleError("Failed to allocate memory for buffers.");
    }

    try {
        // Записываем строки в буферы
        u8 **into = Bu8idle(output);
        strncpy((char*)*into, input1, strlen(input1));
        *into += strlen(input1);

        // Преобразуем строки в формат TLV
        call(RDXJdrain, Bu8idle(input), Bu8cdata(output));
        Breset(output);
        call(TLVsplit, B$u8cidle(ins), Bu8cdata(input));

        // Выполняем слияние
        call(RDXY,    Bu8idle(mergeBuf), B$u8cdata(ins));
        B$u8cunmap(ins);
        B$u8cmap(ins, RDXY_MAX_INPUTS);
        call(TLVsplit, B$u8cidle(ins), Bu8cdata(mergeBuf));

        // Преобразуем результат обратно в формат JDR
        a$dup($u8c, in, B$u8cdata(ins));
        call(RDXJfeed, Bu8idle(output), **in);
        ++*in;
        $eat(in) {
            call($u8feed2, Bu8idle(output), ',', '\n');
            call(RDXJfeed, Bu8idle(output), **in);
        }

        // Преобразуем JDR результат в char*
        size_t resultLength = $len(Bu8cdata(output));
        result = (char*)malloc(resultLength + 1);
        strncpy(result, (char*)*Bu8cdata(output), resultLength);
        result[resultLength] = '\0';

        call($u8feed1, Bu8idle(output), '\n');

        // Выводим результат на консоль
        call(FILEfeedall, STDOUT_FILENO, Bu8cdata(output));
        std::cout << result << std::endl;
        std::cout << "finish" << std::endl;
    } catch (...) {
        handleError("An unexpected error occurred.");
    }

    // Очистка ресурсов
    Bu8unmap(output);
    Bu8unmap(input);
    Bu8unmap(mergeBuf);
    B$u8cunmap(ins);

    free(result);

    return 0;
}

MAIN(my_merge_main)
