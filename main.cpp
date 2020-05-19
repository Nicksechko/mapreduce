#include <string>
#include "mapreduce.h"

int main(int argc, char** argv) {
    std::vector<std::string> pos_args;
    int block_size = 100'000;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "-b") {
            block_size = std::stoi(argv[++i]);
        } else {
            pos_args.emplace_back(argv[i]);
        }
    }

    auto executor = MakeThreadPoolExecutor(4);

    TableFuturePtr result;

    if (pos_args[0] == "map") {
        result = Map(executor, DummyFuture(pos_args[1]),
                     pos_args[3], false, block_size);
    } else if (pos_args[0] == "sort") {
        result = Sort(executor, DummyFuture(pos_args[1]),
                      false, block_size);
    } else if (pos_args[0] == "reduce") {
        result = Reduce(executor, DummyFuture(pos_args[1]),
                        pos_args[3], false, block_size);
    } else if (pos_args[0] == "mapreduce") {
        result = MapReduce(executor, DummyFuture(pos_args[1]),
                           pos_args[3], pos_args[4], false, block_size);
    }

    bp::system("mv " + result->get() + " " + pos_args[2]);

    return 0;
}
