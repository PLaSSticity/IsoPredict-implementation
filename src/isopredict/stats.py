import glob
import os
import pandas as pd

COL_NAME = "Benchmark"
COL_PREDICTION = "Predicted Execution"
COL_EVENT = "Total Events"
COL_TX = "Total Transactions"
COL_GENCON = "Constraint Generation"
COL_SOLVE = "Constraint Solving"
COL_LITERAL = "SMT Literals"

class Stats:
    def __init__(self, stats_dir, output):
        self.filepath = stats_dir
        self.output = output
        self.benchname = stats_dir.rstrip("/").split("/")[-1]
        self.df = parse_csv(stats_dir)

    def print_summary(self):
        sat = self.df.loc[self.df[COL_PREDICTION] == "Yes"]
        unsat = self.df.loc[self.df[COL_PREDICTION] == "No"]
        unknown = self.df.loc[self.df[COL_PREDICTION] == "unknown"]

        print("Avg. constraint generation time: %.1f"%self.df[COL_GENCON].mean())

        if sat[COL_NAME].count() != 0:
            print("Avg. solving time (sat): %.1f"%sat[COL_SOLVE].mean())

        if unsat[COL_NAME].count() != 0:
            print("Avg. solving time (unsat): %.1f"%unsat[COL_SOLVE].mean())

        if unknown[COL_NAME].count() != 0:
            print("Avg. solving time (unknown): %.1f"%unknown[COL_SOLVE].mean())
        

    def to_tex_cmd(self):
        bench = "bank"
        strategy = "full"
        consistency = "cc"

        if "voter" in self.filepath:
            bench = "vote"
        elif "wikipedia" in self.filepath:
            bench = "wiki"
        elif "tpcc" in self.filepath:
            bench = "tpcc"

        if "express" in self.filepath:
            strategy = "exp"
        elif "relaxed" in self.filepath:
            strategy = "rel"

        if "readcommitted" in self.filepath:
            consistency = "rc"

        out_file = os.path.join(self.output, "%s%s_%s.tex"%(bench, strategy, consistency))

        sat = self.df.loc[self.df[COL_PREDICTION] == "Yes"]
        unsat = self.df.loc[self.df[COL_PREDICTION] == "No"]
        unknown = self.df.loc[self.df[COL_PREDICTION] == "unknown"]

        with open(out_file, "w") as out:
            out.write("%% %s\n"%(self.bench))
            out.write("\\newcommand{\\%s%sgencon}{%.1f}\n"%(bench, strategy, self.df[COL_GENCON].mean()))
            out.write("\\newcommand{\\%s%scntsat}{%d}\n"%(bench, strategy, sat[COL_NAME].count()))
            if sat[COL_NAME].count() != 0:
                out.write("\\newcommand{\\%s%seventsat}{%.1f}\n"%(bench, strategy, sat[COL_EVENT].mean()))
                out.write("\\newcommand{\\%s%stxsat}{%.1f}\n"%(bench, strategy, sat[COL_TX].mean()))
                out.write("\\newcommand{\\%s%sgenconsat}{%.1f}\n"%(bench, strategy, sat[COL_GENCON].mean()))
                out.write("\\newcommand{\\%s%ssolvesat}{%.1f}\n"%(bench, strategy, sat[COL_SOLVE].mean()))
            else:
                out.write("\\newcommand{\\%s%seventsat}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%stxsat}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%sgenconsat}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%ssolvesat}{}\n"%(bench, strategy))

            out.write("\\newcommand{\\%s%scntunsat}{%d}\n"%(bench, strategy, unsat[COL_NAME].count()))
            if unsat[COL_NAME].count() != 0:
                out.write("\\newcommand{\\%s%seventunsat}{%.1f}\n"%(bench, strategy, unsat[COL_EVENT].mean()))
                out.write("\\newcommand{\\%s%stxunsat}{%.1f}\n"%(bench, strategy, unsat[COL_TX].mean()))
                out.write("\\newcommand{\\%s%sgenconunsat}{%.1f}\n"%(bench, strategy, unsat[COL_GENCON].mean()))
                out.write("\\newcommand{\\%s%ssolveunsat}{%.1f}\n"%(bench, strategy, unsat[COL_SOLVE].mean()))
            else:
                out.write("\\newcommand{\\%s%seventunsat}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%stxunsat}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%sgenconunsat}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%ssolveunsat}{}\n"%(bench, strategy))

            out.write("\\newcommand{\\%s%scntunknown}{%d}\n"%(bench, strategy, unknown[COL_NAME].count()))
            if unknown[COL_NAME].count() != 0:
                out.write("\\newcommand{\\%s%seventunknown}{%.1f}\n"%(bench, strategy, unknown[COL_EVENT].mean()))
                out.write("\\newcommand{\\%s%stxunknown}{%.1f}\n"%(bench, strategy, unknown[COL_TX].mean()))
                out.write("\\newcommand{\\%s%sgenconunknown}{%.1f}\n"%(bench, strategy, unknown[COL_GENCON].mean()))
                out.write("\\newcommand{\\%s%ssolveunknown}{%.1f}\n"%(bench, strategy, unknown[COL_SOLVE].mean()))
            else:
                out.write("\\newcommand{\\%s%seventunknown}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%stxunknown}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%sgenconunknown}{}\n"%(bench, strategy))
                out.write("\\newcommand{\\%s%ssolveunknown}{}\n"%(bench, strategy))

def parse_csv(dir):
    benchname = dir.rstrip("/").split("/")[-1]
    all_files = glob.glob(os.path.join(dir, "*.csv"))
    li = []

    for f in all_files:
        # print("reading: %s"%f)
        df = pd.read_csv(f, index_col=None, header=0)
        li.append(df)

    df = pd.concat(li, axis=0, ignore_index=True)

    return df
