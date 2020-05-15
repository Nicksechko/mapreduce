import numpy as np


size = 100000
mapping = np.vectorize({1: "lol",
                        2: "kek",
                        3: "cheburek"}.get)

data = np.random.randint(1, 4, size)
borders = np.sort(np.random.choice(size - 1, 1087, replace=False) + 1)
with open("huge_input.txt", 'w') as out:
    last = 0
    for border in borders:
        out.write("key\t")
        np.savetxt(out, mapping(data[last:border]), newline=" ", fmt="%s")
        out.write("\n")
        last = border

    out.write("key\t")
    np.savetxt(out, mapping(data[last:size]), newline=" ", fmt="%s")
    out.write("\n")

with open("correct_huge.txt", 'w') as ans:
    values, counts  = np.unique(data, return_counts=True)
    values = mapping(values)
    for value, count in sorted(zip(values, counts)):
        ans.write(f"{value}\t{count}\n")

