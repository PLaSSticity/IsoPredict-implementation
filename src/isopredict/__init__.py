import argparse
from multiprocessing import set_start_method
import isopredict.graph
import isopredict.strategy
import isopredict.benchmark
import isopredict.datastore as datastore
import isopredict.verify as verify
import isopredict.analysis as predictive
import isopredict.stats as stats
from isopredict.strategy import Strategy, Consistency, EnumAction

def main():
    parser = argparse.ArgumentParser(description='Predicts unserializable behaviors that conforms to a weak isolation level')
    parser.add_argument('filepath')
    parser.add_argument('-c', '--check', action='store_true', help='check serializability of observed execution')
    parser.add_argument('-s', '--stat', action='store_true', help='show statistics of observed execution')
    parser.add_argument('-d', '--debug', action='store_true', help='print debug info to files')
    parser.add_argument('-b', '--bound', help='max distance between predicted write and observed write')
    parser.add_argument('-v', '--visualize', action='store_true', help='visualize commit order')
    parser.add_argument('-l', '--level', type=Consistency, action=EnumAction, help='weak isolation level')
    parser.add_argument('-t', '--tactic', type=Strategy, action=EnumAction, help='strategy of predictive analysis')
    parser.add_argument('-o', '--output', type=str, help='Location of outputs')
    args = parser.parse_args()

    filename = args.filepath
    db = datastore.parse_log(filename)
    if db is None:
        return

    check = args.check
    stats = args.stat
    output = args.output if args.output is not None else "out"
    bound= int(args.bound) if args.bound is not None else db.transaction_count()
    vis = args.visualize
    tactic = args.tactic if args.tactic is not None else Strategy.Full
    level = args.level if args.level is not None else Consistency.Causal
    debug = args.debug


    if stats:
        db.show_stats()
    else:
        if check:
            verifier = verify.Verifier(db, visualize=vis, debug=debug, consistency=level, output=output)
            verifier.verify()
        else:
            analysis = predictive.Analysis(db, bound=bound, visualize=vis, strategy=tactic, debug=debug, consistency=level, output=output)
            analysis.predict()

def runbench():
    set_start_method("spawn")
    
    parser = argparse.ArgumentParser(description='Benchmark Script')
    parser.add_argument('benchmarkdir')
    parser.add_argument('-l', '--level', type=Consistency, action=EnumAction, help='weak isolation level')
    parser.add_argument('-t', '--tactic', type=Strategy, action=EnumAction, help='strategy of predictive analysis')
    parser.add_argument('-o', '--output', type=str, help='Location of outputs')
    parser.add_argument('-mp', '--multi', action='store_true', help='enable parallel benchmark runs through multi-processing, might hang when running from Docker')
    
    args = parser.parse_args()
    dir = args.benchmarkdir
    level = args.level if args.level is not None else Consistency.Causal
    output = args.output if args.output is not None else "out"
    mp = args.multi
    tactic = args.tactic

    if tactic is None:
        tactic = Strategy.Full

    isopredict.benchmark.run_benchmarks(tactic, level, dir, output=output, mp=mp)

def benchstats():
    parser = argparse.ArgumentParser(description='IsoBench Statistics Collector')
    parser.add_argument('filepath')
    parser.add_argument('-o', '--output', type=str, help='Location of outputs')

    args = parser.parse_args()
    filepath = args.filepath
    output = args.output if args.output is not None else "."

    data = stats.Stats(filepath, output)
    data.print_summary()