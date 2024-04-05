# IsoPredict: Dynamic Predictive Analysis for Detecting Unserializable Behaviors in Weakly Isolated Data Store Applications

This is the artifact for the PLDI 2024 paper *IsoPredict: Dynamic Predictive Analysis for Detecting Unserializable Behaviors in Weakly Isolated Data Store Applications*.
IsoPredict takes the execution trace of an application that is backed by a weakly isolated key-value data store, and then predicts an unserializable execution of that application.

IsoPredict is written in Python and it uses Z3Py for its predictive analysis.
Its input and output are in a plain text format that represents the execution trace of a key-value data store application.
It supports `read`, `write`, `insert`, `contains` and `delete` operations on a key-value interface:
- `READ KEY[k] Txn(tx1) From(tx2)` means transaction `tx1` reads `k` from transaction `tx2`'s write.
- `WRITE KEY[k] Txn(tx3)` means transaction `tx3` writes `k`.
- `INSERT[k] to Set[s] Txn(tx4)` means transaction `tx4` inserts `k` to a set whose key is `s`.
- `CONTAINS[k] in Set[s] From(tx4) Txn(tx5)` means transaction `tx5` issues a `contains` query to check whether `k` exists in the set `s`, and that `tx5` is reading from the operations performed by `tx4`.
- `DELETE KEY[k] From Set(s) Txn(tx6)` means transaction `tx6` removes `k` from set `s`.

IsoPredict generates SMT constraints that aims at predicting an unserializable execution based on the given input traces.
If such a prediciton exists, IsoPredict will encode the predicted execution in the same trace format and put it in a file called `unserializable_history_<input trace name>.txt` inside `./out` folder by default.
It will also print out the difference between the predicted trace and the input trace in the terminal.

# Install Dependencies  

`$ apt install python3-pip`

`$ apt install graphviz`

`$ pip3 install -r requirements.txt`

# Installation of IsoPredict

The following command will build and install IsoPredict as a system-wide Python package.

`$ python3 -m build`

`$ pip3 install .`

# Usage  

IsoPredict comes with 3 different commands:
- isopredict: This is the entrypoint to both predictive analysis and isolation level verification.
- isobench: A benchmark script for testing isopredict and for collecting its performance data.
- isostat: This script turns raw data from isobench into .tex commands or human-readable printouts.

```
isopredict [-h] [-c] [-s] [-d] [-b BOUND] [-v]
                  [-l {causal,readcommitted}] [-t {full,express,relaxed}]
                  [-o OUTPUT]
                  filepath

Predicts unserializable behaviors that conforms to a weak isolation level

positional arguments:
  filepath

options:
  -h, --help            show this help message and exit
  -c, --check           check serializability of observed execution
  -s, --stat            show statistics of observed execution
  -d, --debug           print debug info to files
  -b BOUND, --bound BOUND
                        max distance between predicted write and observed
                        write
  -v, --visualize       visualize commit order
  -l {causal,readcommitted}, --level {causal,readcommitted}
                        weak isolation level
  -t {full,express,relaxed}, --tactic {full,express,relaxed}
                        strategy of predictive analysis
  -o OUTPUT, --output OUTPUT
                        Location of outputs
```

```
isobench [-h] [-l {causal,readcommitted}] [-t {full,express,relaxed}]
                [-o OUTPUT] [-mp]
                benchmarkdir

Benchmark Script

positional arguments:
  benchmarkdir

options:
  -h, --help            show this help message and exit
  -l {causal,readcommitted}, --level {causal,readcommitted}
                        weak isolation level
  -t {full,express,relaxed}, --tactic {full,express,relaxed}
                        strategy of predictive analysis
  -o OUTPUT, --output OUTPUT
                        Location of outputs
  -mp, --multi          enable parallel benchmark runs through multi-
                        processing, might hang when running from Docker
```

```
isostat [-h] [-o OUTPUT] filepath

IsoBench Statistics Collector

positional arguments:
  filepath

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Location of outputs
```

# Examples  

There are several simple traces in `tests/microbenchmarks/`. 
For example, `simple_serial_2s3t.txt` contains the following traces that are *serializable*.
```
WRITE KEY[x] Txn(0, 0)
WRITE KEY[y] Txn(0, 0)
WRITE KEY[y] Txn(1, 0)
READ KEY[x] Txn(1, 0) From(0, 0)
WRITE KEY[x] Txn(2, 0)
READ KEY[y] Txn(2, 0) From(1, 0)
```
If we want to know whether such a program has any *unserializable* or buggy behaviors under *causal consistency*, we could ask IsoPredict to find out:

`$ isopredict simple_serial_2s3t.txt`

The output in the terminal is pretty self-explanatory. It's saying IsoPredict has made a prediction by letting transaction T[2, 0] read y from the initial state of the database (T[0, 0]).
```
Parsing log file: simple_serial_2s3t.txt
Predictive: sat
Session Boundaries: 
Boundary of Session[0] = 3/2
Boundary of Session[1] = 2/2
Boundary of Session[2] = 2/2
Predicted Read-Write pairs: 
READ{Session[2]Tx[2, 0]/1}(y) = WRITE{Session[0]Tx[0, 0]/1}, was previously reading from WRITE{Session[1]Tx[1, 0]/0}
```

It also provides a trace in the `out` folder called `unserializable_history_simple_serial_2s3t.txt`.
If you want a more straightforward view, you could enable the `-v` option to visualize the predicted trace:

`$ isopredict -v simple_serial_2s3t.txt`.

This will create a graph called `visualization_simple_serial_2s3t.pdf` in the output folder.

# Project Folder Structure
IsoPredict loosely follows a Python project structure that is compatible with pip for easy packaging and installation.

```
.
├── src/isopredict/                  # Source code of IsoPredict
│   ├── __init__.py                  # Entry points of IsoPredict
│   ├── __main__.py
│   ├── analysis.py                  # Predictive analysis
│   ├── benchmark.py                 # Python script for batch-running IsoPredict
│   ├── datastore.py                 # Data structure representing the execution of a data store
│   ├── graph.py                     # A simple graph for visualization
│   ├── stats.py                     # Python script for computing statistics from benchmark results
│   ├── strategy.py                  # Enum types for different prediction strategies
│   └── verify.py                    # Isolation level verifier
├── tests/                           # Various benchmarks
│   ├── microbenchmark/              # Simple execution traces for quick sanity checks
│   ├── monkeydb/                    # The original MonkeyDB artifact (plus transaction logging)
│   └── oltp/                        # OLTP benchmarks and validation module for the IsoPredict paper
├── Dockerfile                       # Docker file for packaging the artifact
├── LICENSE.txt
├── README.md
├── pyproject.toml                   # Python project configurations
└── requirements.txt                 # Python dependencies
```

# Benchmarks

We used the OLTP benchmarks for testing efficiency and performance of IsoPredict.
Since it has more external dependencies, we recommend running these benchmarks on the docker version of IsoPredict.
You could build the docker image yourself by running `docker build -t isopredict .`, or you could download it from [Zenodo](https://zenodo.org/records/10802748).


## Kick the tires

If you downloaded the docker image from Zenodo, you will need to load the image into your local system (commands following a `$` sign are executed on your system directly, while commands following a `#` sign need to be executed in the docker container):

`$ docker load -i ./isopredict.tar.gz`

Then run the docker container in interactive mode:

`$ docker run -it isopredict`

Once **inside the docker**, you can test if the installation is successful by trying the smallbank benchmark:

`# bash collect_traces.sh 4 smallbank`

`collect_traces.sh` will put the traces inside IsoPredict's log folder `/isopredict/tests/oltp/isopredict_logs`.
The traces folder name is the timestamp of when the traces are collected, e.g. `isopredict_logs/2024-03-04-02-17-41/`.
Please keep this path handy because you will need to pass it along to the next script.

`# bash benchmark.sh causal ./isopredict_logs/2024-03-04-02-17-41/ relaxed`

Replace `./isopredict_logs/2024-03-04-02-17-41/` above with your actual traces folder.
Depending on the hardware, execution time ranges from 5 to 15 minutes.
If everything goes successfully, you will see the following output on the terminal:

```
=======================
Benchmark: smallbank
Isolation: causal
Mode: relaxed
Logs: isopredict_logs/prediction_<time of benchmark run>_from_<time of trace collection>/smallbank_relaxed_causal
+++++++++++++++++++++++
Prediction sat: 10
Prediction unsat: 0
Prediction unknown: 0

Validated: 9
Diverged: 10

Avg. constraint generation time: 26.3
Avg. solving time (sat): 0.8
-----------------------
```

## Reproduce paper results

The paper tested IsoPredict's prediction on 4 of the OLTP benchmarks with different combination of configurations, and it compared IsoPredict's performance with MonkeyDB's.
Here's a summary of experiments conducted by the IsoPredict paper:
| Benchmark | # Transactions per session | Weak Isolation Level | Results    |
| --------- | -------------------------- | -------------------- | ---------- |
| OLTP      | 4                          | Causal               | Table 4(a) |
| OLTP      | 8                          | Causal               | Table 4(b) |
| OLTP      | 4                          | Read Committed       | Table 5(a) |
| OLTP      | 8                          | Read Committed       | Table 5(b) |
| MonkeyDB  | 4                          | Causal               | Table 6(a) |
| MonkeyDB  | 8                          | Causal               | Table 6(b) |
| MonkeyDB  | 4                          | Read Committed       | Table 7(a) |
| MonkeyDB  | 8                          | Read Committed       | Table 7(b) |

For the OLTP benchmarks, there is a trace collection phase and a benchmark phase.
Trace collection is done in serializable mode, so the only variable here is the workload of the traces, which is represented by the number of transactions per session.
As a result, experiments of the same workload size could share their input traces.
After collecting the traces, the paper tests IsoPredict's all three prediction strategies on the traces under both Causal Consistency and Read Committed settings.
IsoPredict's performance statistics, such as total amount of predictions found and time spent on constraint solving, etc, will be printed at the end of the benchmark.

The MonkeyDB experiments run the original MonkeyDB artifact on the same OLTP benchmarks with the same configuration as IsoPredict's experiments.
The assertion violation rate and the serializability violation rate will be printed to the console after these experiments.

The following step-by-step guide shows the instructions on running the OLTP benchmark and MonkeyDB experiments under small workload (4 transactions per session, 3 sessions).
To reproduce large workload experiment results (8 transactions per session, 3 sessions), simply change the trace collection parameter from 4 to 8.
Due to scalability issues, these benchmarks may take more than 24 hours to finish in the large workload setting.

### Step 1. Trace Collection
First, collect traces from all 4 benchmarks: smallbank, voter, tpcc, wikipedia.
Make sure you're in the `/isopredict/tests/oltp/` directory inside the Docker container (which is the default working directory when you enter the container in interactive mode).
Then run the following command to collect traces for all 4 benchmarks:

`# bash collect_traces.sh`

This command by default will run all 4 benchmarks with a limit of 4 transactions per session.
Each benchmark will be run 10 times with different random seeds for generating data.
The traces from these benchmark runs will be placed into a folder that looks like `isopredict_logs/2024-03-04-03-04-16`, where the subfolder's name is determined by the machine time of when the traces are collected.
We will run predictive analysis and validation on these traces to reproduce the numbers in Table 4 and part of Table 5.
Please note that due to nondeterminsm, the exact numbers may not match the paper's.

Alternatively, if you would like to run a particular benchmark instead of all 4, you could use the following command:

`# bash collect_traces.sh 4 <benchmark_name>`

, where each benchmark would run in a 3 session, 4 transactions per session setting (default workload), and that `<benchmark_name>` could be either `smallbank`, `voter`, `tpcc` or `wikipedia`.

Similar to the kick-the-tire example, this will place all the traces in a folder whose name is the timestamp of data collection, and is located in `/isopredict/tests/oltp/isopredict_logs/`.
Please keep this path handy as you will need it for the next steps.

In some rare cases, our modified version of MonkeyDB might crash randomly during either trace collection, which might cause some issues when trying to reproduce paper results.
We are not entirely sure about the reason behind the crashes, but it won't affect the end results that much because each benchmark was repeated multiple times.
You can verify if all the traces are collected successfully by manually look into the traces directory.
If one trace file's size is significantly smaller than others, then it's very likely such trace was corrupted.
If that happened, you could try repeat the trace collection for that particular benchmark until the problem goes away.

### Step 2. Table 4(a)
Table 4(a) tests IsoPredict's predictive analysis under *causal consistency*.
We will run IsoPredict on all 4 benchmarks with 10 traces per benchmark.
In addition, we will test IsoPredict's 3 prediction strategies on these traces.
That amounts to a total of 120 predictive analysis runs for this experiment.
The total execution time of this experiment could be more than 12 hours.

`# bash benchmark.sh causal ./isopredict_logs/2024-03-04-03-04-16`

*Don't forget to replace `2024-03-04-03-04-16` with the actual traces directory.*

The results will be printed to the terminal once all the tests are finished.
Each block of data corresponds to a single row in Table 4(a).
They will look like this:
```
=======================
Benchmark: smallbank
Isolation: causal
Mode: Approx-Strict
Logs: isopredict_logs/prediction_<time of benchmark run>_from_<time of trace collection>/smallbank_express_causal
+++++++++++++++++++++++
Prediction sat: 4
Prediction unsat: 6
Prediction unknown: 0

Validated: 4
Diverged: 0

Avg. constraint generation time: 22.9
Avg. solving time (sat): 1.0
-----------------------
=======================
Benchmark: smallbank
Isolation: causal
Mode: Approx-Relaxed
Logs: isopredict_logs/prediction_<time of benchmark run>_from_<time of trace collection>/smallbank_relaxed_causal
+++++++++++++++++++++++
Prediction sat: 10
Prediction unsat: 0
Prediction unknown: 0

Validated: 9
Diverged: 10

Avg. constraint generation time: 26.3
Avg. solving time (sat): 0.8
-----------------------
```

They are also saved to a file called `results.txt` under the `isopredict_logs/prediction_<time of benchmark run>_from_<time of trace collection>` folder.

As mentioned in Step 1, there's a small chance some of the traces might be corrupted due to the database crashing.
If that happened during the trace collection time, you will see some error messages that look like this:

`[Error] trace was corrupted: ./isopredict_logs/2024-03-04-16-45-01/tpcc/2024-03-04-16-52-55_23Q_run_tpcc_3_causal_random_random_90000.txt`

If it crashed during IsoPredict's validation process, you will see a lot of error messages that looks like:
`mv: cannot stat 'log/validation_traces/voter/REPLAY_unserializable*.txt': No such file or directory`

One potential workaround is to repeat that particular benchmark.
For example, if `tpcc` throw any of the errors above, you could repeat Step 1 for `tpcc` only:

`# bash collect_traces.sh 4 tpcc`

This gives you a new trace folder with `tpcc` traces only. 
Let's say its name is `2024-03-04-03-04-20`.

Then you could repeat the experiment with `tpcc`:

`# bash benchmark.sh causal ./isopredict_logs/2024-03-04-03-04-20`

### Step 3. Table 5(a)
This one is essentially the same as Table 4(a), except we are testing predictions made under *read committed" instead of *causal consistency*.
The execution could also take more than 12 hours.

`# bash benchmark.sh readcommitted ./isopredict_logs/2024-03-04-03-04-16`

*And again, don't forget to replace `2024-03-04-03-04-16` with the actual traces directory.*

### Step 4. Table 6(a)

Table 5(a) runs the same benchmarks as Step 2's on the original MonkeyDB.
For each benchmark, it runs 30 trials, which means a total of 120 for all 4 benchmarks.
It typically takes a few hours for all 120 trials to finish.

First, go to `/isopredict/tests/monkeydb` directory inside the Docker container.
Then run the following command for all 4 benchmarks:

`# bash comparison.sh smallbank,voter,tpcc,wikipedia`

Or if you wish to run them one by one.
You could do `# bash comparison.sh <benchmark_name>`, where `<benchmark_name>` is either `smallbank`, `voter`, `tpcc` or `wikipedia`.

The output represents the middle column in Table 5(a).
For the rightmost column of Table 5(a), you will need to refer to the results from Step 2.
Because of the non-deterministic nature of MonkeyDB, the exact numbers may not match the paper's.

```
----------------------------------------------
Benchmark: smallbank Assertion Failure: 80.00% Unserializable: 100.00%
----------------------------------------------
```

### Step 5. Table 7(a)

To reproduce Table 5(b), you will need to run the following command inside the Docker container's `/isopredict/tests/monkeydb` directory:

`# bash comparison.sh smallbank,voter,tpcc,wikipedia readcommitted`

The format of the reuslts is the same as the one from Step 4.