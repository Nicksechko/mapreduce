#include <string>
#include "mapreduce.h"

int main(int argc, char** argv) {
    std::string mode(argv[1]);
    std::string script_path(argv[2]);
    std::string source_path(argv[3]);
    std::string result_path(argv[4]);

    auto executor = MakeThreadPoolExecutor(2);

    TableTaskPtr result;

    if (mode == "map") {
        std::string tmp_result = "tmp_result";
        auto map_result = Map(executor, source_path, tmp_result, script_path);
        result = Sort(executor, map_result->get(), result_path, 1, true);
        std::cout << result->get() << std::endl;
    }

    return 0;
}
