#include <string>
#include "mapreduce.h"

int main(int argc, char** argv) {
    std::string mode(argv[1]);
    std::string source_path(argv[2]);
    std::string result_path(argv[3]);
    std::string first_script_command;
    if (argc > 4) {
        first_script_command = argv[4];
    }
    std::string second_script_command;
    if (argc > 5) {
        second_script_command = argv[5];
    }

    auto executor = MakeThreadPoolExecutor(2);

    TableFuturePtr result;

    if (mode == "map") {
        result = Map(executor, DummyFuture(source_path), first_script_command);
    } else if (mode == "sort") {
        result = Sort(executor, DummyFuture(source_path));
    } else if (mode == "reduce") {
        result = Reduce(executor, DummyFuture(source_path), first_script_command);
    } else if (mode == "mapreduce") {
        result = MapReduce(executor, DummyFuture(source_path), first_script_command, second_script_command);
    }

    bp::system("mv " + result->get() + " " + result_path);

    return 0;
}
