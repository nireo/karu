# karu: linux database written in c++

## Usage
```cpp
#include "karu.h"
#include <iostream>

int main() {
    karu::DB db("./database_folder");
    if (auto status = db.Insert("hello", "world"); !status.ok()) {
        std::cerr << "error inserting value to map: " << status.message() << '\n';
        return 1;
    }

    auto status = db.Get("hello");
    if (!status.ok()) {
        std::cerr << "error getting value from map: " << status.message() << '\n';
        return 1;
    }

    std::cout << "Got value: " << *status << '\n';
    return 0;
}
```

## Unit Tests

```
chmod +x run_tests.sh
cmake -S . -B build/
./run_tests.sh
```
