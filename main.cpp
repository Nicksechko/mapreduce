#include <string>
#include "mapreduce.h"

int main(int argc, char** argv) {
    std::string mode(argv[1]);
    std::string source_path(argv[2]);
    std::string result_path(argv[3]);

    auto executor = MakeThreadPoolExecutor(4);

    if (mode == "map") {
        std::string script_path(argv[4]);
        auto map_result = Map(executor, source_path, result_path, script_path);
        std::cout << map_result->get() << std::endl;
    } else if (mode == "sort") {
        auto sort_result = Sort(executor, source_path, result_path);
        std::cout << sort_result->get() << std::endl;
    } else if (mode == "reduce") {
        std::string script_path(argv[4]);
        auto reduce_result = Reduce(executor, source_path, result_path, script_path);
        std::cout << reduce_result->get() << std::endl;
    }

    return 0;
}
