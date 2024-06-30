
import cProfile
import pstats
import sys
import os


sys.path.insert(1, os.path.join(sys.path[0], "../.."))


with cProfile.Profile(subcalls=False) as pr:

    import tarchia.main
#    app = tarchia.main.application

#        suite = open("tests/sql_battery/test_shapes_and_errors_battery.py").read()
#        sys.path.insert(1, os.path.join(sys.path[0], ".."))
#        exec(suite)

    #stats = pstats.Stats(pr).strip_dirs().sort_stats("tottime")
    stats = pstats.Stats(pr).sort_stats("tottime")

    func_list = [(k, v) for k, v in stats.stats.items() if "tarchia" in k[0] and "." in k[0] and (not "debugging.py" in k[0] and not "brace.py" in k[0])]
    sorted_funcs = sorted(func_list, key=lambda x: x[1][2], reverse=True)  # Sorted by total time

    header = ["Line", "Function", "Calls", "Total (ms)", "Cumulative (ms)"]
    divider = "-" * 110
    print(divider)
    print("{:<45} {:<20} {:>10} {:>12} {:>15}".format(*header))
    print(divider)

    limit = 25
    for func, func_stats in sorted_funcs:
        file_name, line_number, function_name = func
        total_calls, _, total_time, cumulative_time, _ = func_stats
        file_name = file_name.split("opteryx")[-1]
        file_name = "..." + file_name[-37:] if len(file_name) > 40 else file_name
        function_name = function_name[:17] + "..." if len(function_name) > 20 else function_name
        row = [f"{file_name}:{line_number}", function_name, total_calls, f"{(total_time * 1000):.6f}", f"{(cumulative_time * 1000):.6f}"]
        print("{:<45} {:<20} {:>10} {:>12} {:>15}".format(*row))
        limit -= 1
        if limit == 0:
            break
    print(divider)

quit()
import json

from opteryx.third_party import sqloxide
parsed_statements = sqloxide.parse_sql(SQL, dialect="mysql")
print(json.dumps(parsed_statements))

