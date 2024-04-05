import csv
import functools
import glob
import multiprocessing
from multiprocessing import set_start_method
from pathlib import Path
from tabulate import tabulate
from z3 import *
import isopredict.datastore as datastore
import isopredict.verify as verify
import isopredict.analysis as predictive
from isopredict.strategy import Consistency, Strategy, EnumAction

def run(filename, level, tactic, output):
    db = datastore.parse_log(filename)
    if db is None:
        return None
    
    bound = 10
    # verifier = verify.Verifier(db, consistency=level, output=output)
    analysis = predictive.Analysis(db, bound=bound, consistency=level, strategy=tactic, output=output)

    # serial = verifier.verify()
    predicted = "N/A"
    time_gencon = "N/A"
    time_solve = "N/A"
    transaction_cnt = "%d"%(db.transaction_count())
    event_cnt = "%d"%(db.event_count())

    predicted = analysis.predict()
    time_gencon = analysis.time_gencon
    time_solve = analysis.time_solve
    
    # if serial == sat:
    #     serial = "Serializable"
    # elif serial == unsat:
    #     serial = "Not Serializable"
    # else:
    #     serial = "unknown"

    if predicted == "N/A":
        predicted = "N/A"
    elif predicted == sat:
        predicted = "Yes"
    elif predicted == unsat:
        predicted = "No"
    else:
        predicted = "unknown"

    out_file = "%s/%s_%s_%s.csv"%(output, db.in_file, str(level).split(".")[-1].lower(), str(tactic).split(".")[-1].lower())
    headers = ["Benchmark", "Observed Execution", "Predicted Execution", "Constraint Generation", "Constraint Solving", "Total Events", "Total Transactions"]
    values = [db.in_file, "N/A", predicted, time_gencon, time_solve, event_cnt, transaction_cnt]

    with open(out_file, "w") as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        writer.writerow(values)

    return values

def run_benchmarks(tactic, level, dir="./microbenchmark", output="./out", mp=False):
    bench_name = "%s_%s_%s"%(dir.rstrip("/").split("/")[-1], str(tactic).split(".")[-1].lower(), str(level).split(".")[-1].lower())
    bench_dir = "./%s/%s"%(output, bench_name)
    Path(bench_dir).mkdir(parents=True, exist_ok=True)

    out_file = "%s/%s.tex"%(bench_dir, bench_name)
    table = []
    headers=["Benchmark", "Observed Execution", "Predicted Execution", "Constraint Generation", "Constraint Solving", "Total Events", "Total Transactions"]

    files = []
    for file in glob.glob("%s/%s"%(dir, "*.txt")):
        files.append(file)

    if mp:
        cpu_count = multiprocessing.cpu_count()
        if cpu_count > 5:
            cpu_count = 5
        else:
            cpu_count -= 1

        with multiprocessing.Pool(cpu_count, maxtasksperchild=1) as p:
            table = p.map(functools.partial(run, level=level, tactic=tactic, output=bench_dir), files)
    else:
        for f in files:
            result = run(f, level=level, tactic=tactic, output=bench_dir)
            if result is not None:
                table.append(result)

    # for f in files:
    #     table.append(run(f, tactic))

    with open(out_file, "w") as out:
        out.write(tabulate(sorted(table), headers, tablefmt="latex"))
