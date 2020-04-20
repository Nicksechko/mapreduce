#include <iostream>
#include <cassert>
#include <optional>

int main() {
    std::string key;
    std::optional<std::string> prev_key;
    int64_t sum = 0;
    while (std::getline(std::cin, key, '\t')) {
        int64_t value;
        std::cin >> value;
        sum += value;
        assert(!prev_key.has_value() || prev_key == key);
        prev_key = key;
        std::getline(std::cin, key);
    }
    std::cout << prev_key.value() << "\t" << sum << "\n";
    return 0;
}
