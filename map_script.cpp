#include <iostream>
#include <sstream>

int main() {
    std::string key;
    std::string value;
    while (std::getline(std::cin, key, '\t')) {
        std::getline(std::cin, value);
        std::istringstream values(value);
        std::string name;
        while (values >> name) {
            std::cout << name << "\t1\n";
        }
    }

    return 0;
}
