#include <sstream>
#include <string>

#include "karu.h"

void parse_input();

int main(int argc, char **argv) {
  if (argc != 2) {
    std::cerr << "karu: You need to provide path to the database file.\n";
    return 1;
  }

  auto db = karu::DB(std::string(argv[2]));

  std::string line;
  while (std::getline(std::cin, line)) {
    std::stringstream ss(line);
    std::string action;
    std::getline(ss, action, ' ');

    if (action == "set") {
      std::string key, val;
      std::getline(ss, key, ' ');
      std::getline(ss, val, ' ');

      auto status = db.Insert(key, val);
      if (!status.ok()) {
        std::cerr << "karu: Error inserting key-value pair:\n"
                  << status.message() << '\n';
        return 1;
      }
      std::cout << "set value\n";
    } else if (action == "get") {
      std::string key;
      std::getline(ss, key, ' ');

      auto status = db.Get(key);
      if (!status.ok()) {
        std::cerr << "karu: Error getting key-value pair:\n"
                  << status.status().message() << '\n';
        return 1;
      }
      std::cout << status.value() << '\n';

    } else {
      std::cerr << "karu: Wrong type of action.\n";
      return 1;
    }
  }

  return 0;
}
