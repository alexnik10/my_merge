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

int main() {
    // Исходные строки в формате RDX-JDR
    const char* input1 = "<1:2>,<1:1:3>";
    const char* input2 = "{3:4,4:5,\"seven\"}";

    Bu8 output = {};        // Инициализируем пустым значением
    Bu8 input1Buffer = {};  // Инициализируем пустым значением
    Bu8 input2Buffer = {};  // Инициализируем пустым значением
    B$u8c ins = {};         // Инициализируем пустым значением

    // Инициализация буферов
    if (!Bu8map((u8 *const *)&output, 1UL << 32) || 
        !Bu8map((u8 *const *)&input1Buffer, 1UL << 32) || 
        !Bu8map((u8 *const *)&input2Buffer, 1UL << 32) || 
        !B$u8cmap(($u8c *const *)&ins, RDXY_MAX_INPUTS)) {
        handleError("Failed to allocate memory for buffers.");
    }

    try {
        // Записываем строки в буферы
        if (!FILEfeedall(   STDIN_FILENO, (uint8_t const *const *)Bu8idle(&input1Buffer))) {
            handleError("Failed to feed input buffer.");
            FILEfeedall(Bu8idle(&input2Buffer), input2, strlen(input2));

        // Преобразуем строки в формат TLV
        if (!RDXJdrain(Bu8idle(&input1Buffer), Bu8cdata(&input1Buffer)) || 
            !RDXJdrain(Bu8idle(&input2Buffer), Bu8cdata(&input2Buffer))) {
            handleError("Failed to convert to TLV format.");
        }

        // Добавляем TLV-данные в массив `ins`
        TLVsplit(B$u8cidle(&ins), Bu8cdata(&input1Buffer));
        TLVsplit(B$u8cidle(&ins), Bu8cdata(&input2Buffer));

        // Выполняем слияние
        if (!RDXY(Bu8idle(&output), B$u8cdata(&ins))) {
            handleError("Failed to merge data.");
        }

        // Преобразуем результат обратно в формат JDR
        if (!RDXJfeed(Bu8idle(&output), Bu8cdata(&output))) {
            handleError("Failed to convert to JDR format.");
        }

        // Вывод результата на консоль
        std::cout << reinterpret_cast<char*>(Bu8cdata(&output)) << std::endl;
    } catch (...) {
        handleError("An unexpected error occurred.");
    }

    // Очистка ресурсов
    Bu8unmap(&output);
    Bu8unmap(&input1Buffer);
    Bu8unmap(&input2Buffer);
    B$u8cunmap(&ins);

    return 0;
}
