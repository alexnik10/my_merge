## Добавление merge оператора из rdx в rocksdb
Консольная утилита с примером работы merge-а из [RDX](https://github.com/gritzko/librdx) в merge-операторе [RocksDB](https://github.com/facebook/rocksdb).

### Установка RocksDb
Перед запуском проекта нужно собрать RocksDb и установить недостающие библиотеки.

Чтобы установить необходимые библиотеки, введите следующие команды:
```
sudo apt install libzstd-dev
sudo apt install liblz4-dev
sudo apt install libbz2-dev
sudo apt install zlib1g-dev
sudo apt install libsnappy-dev
sudo apt install libgflags-dev
```

Далее в репозитории переходим в папку с RocksDb и собираем библиотеку:
```
cd rocksdb
make static_lib EXTRA_CXXFLAGS=-frtti
```

### Сборка основной программы
ДЛя того, чтобы собрать программу в Linux, нужно ввести следующие команды в корне репозитория:
```
cmake .
make
```

### Запуск программы
Для запуска утилиты дайте доступ на исполенеие собранному бинарнику, потом запускайте саму программу:
```
chmod +x rdx_merge
./rdx_merge
```