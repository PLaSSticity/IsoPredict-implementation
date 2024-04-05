set -m

function now {
    date +%Y-%m-%d-%H-%M-%S
}

function oltp_run() {
    cd ${CURR_RUN_DIR}
    > $MONKEYDB_LOG
    > $OLTP_LOAD
    > $OLTP_EXEC
    PORT=`tail -f "$MONKEYDB_LOG" | head -n1 | cut -d':' -f2` &
    ${MONKEYDB_DIR}/target/release/monkeydb -a 0 >> "$MONKEYDB_LOG" 2>&1 &
    MONKEYDB_PID=$!
    echo "$MONKEYDB_PID" > "$MONKEYDB_PID_FILE"
    fg 1 > /dev/null
    PORT=`tail -f "$MONKEYDB_LOG" | head -n1 | cut -d':' -f2`
    # echo curr_dir $CURR_RUN_DIR
    # echo port $PORT
    mysql -h 127.0.0.1 -P $PORT -e 'reset';
    oltp_config="${CURR_RUN_DIR}/${BENCHNAME}_config_monkeydb_${PORT}.xml"
    sed "s|<DBUrl>jdbc:mysql://localhost:[0-9]\+/${BENCHNAME}</DBUrl>|<DBUrl>jdbc:mysql://localhost:${PORT}/${BENCHNAME}</DBUrl>|g" "${OLTP_DIR}/config/sample_${BENCHNAME}_config.xml" > "$oltp_config"
    sed -i "s|<isolation>.*</isolation>|<isolation>${CONSISTENCY}</isolation>|g" "${oltp_config}"
    # sed -i "s|<isolation>.*</isolation>|<isolation>TRANSACTION_READ_COMMITTED</isolation>|g" "${oltp_config}"
    sed -i "s|<username>.*</username>|<username>root</username>|g" "${oltp_config}"
    sed -i "s|<password>.*</password>|<password></password>|g" "${oltp_config}"
    sed -i "s|<terminals>[0-9]\+</terminals>|<terminals>${NTERM}</terminals>|g" "${oltp_config}"
    sed -i "s|<time>[0-9]\+</time>|<time>${DUR}</time>|g" "${oltp_config}"
    cd $OLTP_DIR
    ${OLTP_DIR}/oltpbenchmark -b "${BENCHNAME}" -c "${oltp_config}" --create=true --load=true -o "$OLTP_OUTD" > "${OLTP_LOAD}" 2> "${OLTP_LOAD}_err"
    cd - > /dev/null
    if [ -s "${OLTP_LOAD}_err" ]; then
        echo "found error in LOAD - ${OLTP_LOAD}_err"
    else
        # mysql -h 127.0.0.1 -P $PORT -e "UPDATE useracct SET user_editcount = 0; COMMIT;";
        mysql -h 127.0.0.1 -P $PORT -e "loading reset";
        mysql -h 127.0.0.1 -P $PORT -e "set consistency ${CONSISTENCY}";
        mysql -h 127.0.0.1 -P $PORT -e "read ${EXEC_ST}";
        cd $OLTP_DIR
        ${OLTP_DIR}/oltpbenchmark -b "${BENCHNAME}" -c "${oltp_config}" --execute=true -o "$OLTP_OUTD" -rng "${SEED}" >> "${OLTP_EXEC}" 2> "${OLTP_EXEC}_err"
        cd - > /dev/null
        mysql -h 127.0.0.1 -P $PORT -e 'print summary';
        if [ -s "${OLTP_EXEC}_err" ]; then
            echo "found error in EXEC - ${OLTP_EXEC}_err"
        else
            ${MONKEYDB_DIR}/target/release/examples/${BENCHNAME}_cr -p "$PORT" -s "${ASSERT_ST}" -d "${BENCHNAME}"
        fi
    fi
    # echo $MONKEYDB_PID
    kill "$MONKEYDB_PID"
    fg 2 > /dev/null && echo "monkeydb exited"
    cd $MONKEYDB_DIR
}

function setup_oltp_run {
    PORT=3306
    NTERM=7
    DUR=4
    CONSISTENCY="causal"
    EXEC_ST="random"
    ASSERT_ST="random"
    # ISOPredict
    SEED=0

    [ -z $1 ] || BENCHNAME=$1
    [ -z $2 ] || NTERM=$2
    [ -z $3 ] || DUR=$3
    [ -z $4 ] || CONSISTENCY=$4
    [ -z $5 ] || SEED=$5
    [ -z "$6" ] || EXEC_ST="$6"
    [ -z "$7" ] || ASSERT_ST="$7"

    CURR_RUN_DIR=`mktemp -d $RUN_DIR/$(now)_XXX_run_${BENCHNAME}_${NTERM}_${CONSISTENCY}_${EXEC_ST}_${ASSERT_ST}_${SEED}`
    CURR_RUN=$(basename $CURR_RUN_DIR)

    CURR_TRACE_DIR=${TRACE_DIR}/$BENCHNAME
    mkdir -p ${CURR_TRACE_DIR}

    MONKEYDB_LOG=${CURR_RUN_DIR}/monkeydb_log
    OLTP_LOAD=${CURR_RUN_DIR}/oltp_load_log
    OLTP_EXEC=${CURR_RUN_DIR}/oltp_exec_log
    MONKEYDB_PID_FILE=${CURR_RUN_DIR}/monkeydb_pid
    OLTP_OUTD=oltp_out
    EXEC_TRACE=${CURR_TRACE_DIR}/${CURR_RUN}.txt
    oltp_run

    # ISOPredict
    grep -iE '(key|insert|delete|contains)\[.*\]' ${MONKEYDB_LOG} > ${EXEC_TRACE}
}

function run_bench() {
    nodes=$1
    consistency=$2
    bench=$3
    total_run=$4
    timelimit=$5
    curr_violated_log_dir=`mktemp -d ${ASSERT_DIR}/$(now)_XXX`
    dur=0
    for i in `seq 1 ${total_run}`; do
        # progressbar
        p=$((20 * (i-1) / total_run))
        seed=$((i * 10000))
        printf "\e[KProgress on curr param: " >&2
        printf "%-*s" $((p)) '[' | tr ' ' '#' >&2
        printf "%*s%3d%%\r"  $((20-p))  ']' "$((p*5))" >&2

        start=`date +%s`
        setup_oltp_run ${bench} ${nodes} "${timelimit}" "${consistency}" "${seed}" >> "${curr_violated_log_dir}/${bench}.out" 2>&1
        end=`date +%s`
        dur=$(($dur + $end - $start))
    done
    printf "\e[K" >&2
    echo "=========="
    echo "Benchmark: ${bench}"
    echo "----------"
    echo "${total_run} runs with limit of ${timelimit} transactions"
    echo "On ${nodes} nodes with \"${consistency}\" consistency"
    echo "Average duration per run: $(( $dur / $total_run )) secs"
    echo "----------------------------------------------"
}


function dep_check_one {
    [ -z $1 ] || which $1 > /dev/null 2>&1 || echo "  $1"
}

function dep_check_all {
    which > /dev/null 2>&1
    # 127 is for command not found
    # https://www.gnu.org/software/bash/manual/html_node/Exit-Status.html
    if [ "$?" -ne 127 ]; then
        for e in awk cat column cut date grep head mktemp mysql printf sort uniq; do
            dep_check_one $e
        done
    else
        echo " which (for dependency check)"
    fi
}

check=`dep_check_all`
[ ! -z "$check" ] && echo -e "not found:\n$check\nmake sure they are available." && exit 1

LOG_DIR=`readlink -f log`

mkdir -p ${LOG_DIR}

RUN_DIR="${LOG_DIR}/monkeydb_runs"
mkdir -p ${RUN_DIR}

ASSERT_DIR="${LOG_DIR}/monkeydb_asserts"
mkdir -p ${ASSERT_DIR}

TRACE_DIR="${LOG_DIR}/traces"
mkdir -p ${TRACE_DIR}

OLTP_DIR=`readlink -f oltpbench`
MONKEYDB_DIR=`readlink -f .`

# DEFAULT PARAMETERS
total_run=5
benches="tpcc,smallbank,voter,wikipedia"
consistencies="readcommitted,causal"
nodes="2,3"
timelimit="10"

[ -z $1 ] || total_run=$1
[ -z $2 ] || benches=$2
[ -z $3 ] || consistencies=$3
[ -z $4 ] || nodes=$4
[ -z $5 ] || timelimit=$5

for node in ${nodes//,/ }; do
    for consistency in ${consistencies//,/ }; do
        for bench in ${benches//,/ }; do
            run_bench "$node" "$consistency" "$bench" "$total_run" "$timelimit"
        done
    done
done
