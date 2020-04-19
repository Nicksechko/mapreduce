#include <iostream>
#include <string>
#include <thread>
#include <shared_mutex>
#include "mapreduce.h"

int main(int argc, char** argv) {
    std::string mode(argv[1]);
    std::string script_path(argv[2]);
    std::string source_path(argv[3]);
    std::string result_path(argv[4]);

    auto executor = MakeThreadPoolExecutor(1);

    FuturePtr<std::string> result;

    if (mode == "map") {
        std::string tmp_result = "tmp_result";
        auto map_result = Map(executor, script_path, source_path, tmp_result);
        executor->submit(map_result);
        result = Sort(executor, map_result->get(), result_path);
        executor->submit(result);
        result->get();
    }

    return 0;
}
