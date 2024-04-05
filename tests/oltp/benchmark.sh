function predict() {
    input=$1
    isolation=$2
    tactic=$3
    output=$4

    isobench -t $tactic -l $isolation -o $output $input
}

function replay() {
    bench=$1
    isolation=$2
    prediction=$3
    output_dir=$4
    trace=`basename $prediction`
    trace="REPLAY_${trace%.*}.txt"
    div=`basename $prediction`
    div="DIV_${div%.*}.txt"
    reads=`basename $prediction`
    reads="READS_${reads%.*}.txt"

    seed=`echo "$prediction" | sed -n "s/^.*_\([0-9]*\).txt$/\1/p"`
    
    timeout 1300s bash replay.sh 1 $bench $isolation 3 $seed $prediction
    mv log/validation_traces/${bench}/${trace} $output_dir
    mv log/divergence/${bench}/${div} $output_dir
    mv log/divergence/${bench}/${reads} $output_dir
}

function diverged() {
    DIV=$1
    READS=$2

    fin=`tail -n 1 ${DIV}`
    substring=$(echo "$fin" | grep -o "Total matches.*")
    num1=$(echo "$substring" | cut -d ":" -f 2 | cut -d "/" -f 1)
    num2=$(echo "$substring" | cut -d "/" -f 2)

    replayed=`cat ${DIV} | wc -l`
    comma=`grep -Fo ',' ${READS} | wc -l`
    lines=`cat ${READS} | wc -l`
    expected="$(( comma + lines ))"


    if [ "$replayed" -eq "$expected" ]; then
        if [ "$num1" = "$num2" ]; then
            false
        else
            true
        fi
    else
        true
    fi
}

function validate() {
    isolation=$1
    traces_dir=$2

    cnt=0
    validated=0
    diverge_cnt=0

    for t in $traces_dir/REPLAY*.txt; do
	tmp=`basename $t`
	tmp="temp_${tmp}"

	isopredict -c -l $isolation $t > $traces_dir/$tmp

	if ! grep -q "Serializable: sat" $traces_dir/$tmp
	then
	    (( validated++ ))
	fi

	rm $traces_dir/$tmp

	(( cnt++ ))
    done

    for div in $traces_dir/DIV*.txt; do
        filename=`basename $div`
        expected="${filename/DIV/READS}"
        reads=$traces_dir/$expected

        if diverged $div $reads; then
            (( diverge_cnt++ ))
        fi
    done

    echo "Validated: ${validated}" > $traces_dir/validation.txt
    echo "Diverged: ${diverge_cnt}" >> $traces_dir/validation.txt
}

if [ "$#" -lt 2 ]
then
    echo "At least 2 arguments required, $# provided"
    echo "Usage: benchmark.sh <isolation> <traces_dir> [strategy]"
    exit
fi

ISOLATION=$1
TRACES=$2
TRACES=${TRACES%/}
DIR=`basename $TRACES`
TIME=`date +%Y-%m-%d-%H-%M-%S`
TACTICS="full,express,relaxed"
LOG_DIR=isopredict_logs
CURR_RUN="prediction_${TIME}_from_${DIR}"

[ -z $3 ] || TACTICS=$3

mkdir -p $LOG_DIR
mkdir -p $LOG_DIR/$CURR_RUN


for BENCH in $TRACES/*; do

    benchname=`basename $BENCH`

    for TACTIC in ${TACTICS//,/ }; do
        mode="Exact-Strict"

        if grep -q "express" <<< "$TACTIC"; then
            mode="Approx-Strict"
        fi

        if grep -q "relaxed" <<< "$TACTIC"; then
            mode="Approx-Relaxed"
        fi

        predict $BENCH $ISOLATION $TACTIC "${LOG_DIR}/${CURR_RUN}"

        bench_dir="${LOG_DIR}/${CURR_RUN}/${benchname}_${TACTIC}_${ISOLATION}"
        bench_dir_abs=`realpath $bench_dir`

        sat=0
        total=`ls ${bench_dir_abs}/*.csv | wc -l`

        for f in ${bench_dir}/unserializable*.txt; do
            (( sat++ ))
            f_abs=`realpath $f`
            replay $benchname $ISOLATION $f_abs $bench_dir_abs
        done

        unsat=`grep No ${bench_dir_abs}/*.csv | wc -l`
        unknown=`grep unknown ${bench_dir_abs}/*.csv | wc -l`

        validate $ISOLATION $bench_dir_abs

        v=`grep Validated ${bench_dir_abs}/validation.txt`
        d=`grep Diverged ${bench_dir_abs}/validation.txt`
        out="${bench_dir_abs}/summary.txt"

        echo "=======================" > $out
        echo "Benchmark: $benchname" >> $out
        echo "Isolation: $ISOLATION" >> $out
        echo "Mode: $mode" >> $out
        echo "Logs: $bench_dir" >> $out
        echo "+++++++++++++++++++++++" >> $out
        echo "Prediction sat: $sat" >> $out
        echo "Prediction unsat: $unsat" >> $out
        echo "Prediction unknown: $unknown" >> $out
        echo >> $out
        echo "Validated: ${v//[^0-9.]/}" >> $out
        echo "Diverged: ${d//[^0-9.]/}" >> $out
        echo >> $out
        isostat ${bench_dir_abs} >> $out
        echo "-----------------------" >> $out

    done
done

cat ${LOG_DIR}/${CURR_RUN}/*/summary.txt
cat ${LOG_DIR}/${CURR_RUN}/*/summary.txt > ${LOG_DIR}/${CURR_RUN}/results.txt
echo "Results saved to ${LOG_DIR}/${CURR_RUN}/results.txt"
