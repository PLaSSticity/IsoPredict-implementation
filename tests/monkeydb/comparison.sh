function check_assertions() {
    FILE=$1
    bench=`basename $FILE`
    bench=${bench%.out}
    total=`grep 'Terminated' $1 | wc -l`
    violated=0
    curr=0

    while IFS="" read -r line || [ -n "$line" ]; do
        if grep -q "violated" <<< "$line"; then
            curr=1
        fi

        if grep -q "Terminated" <<< "$line"; then
            if [ $curr -eq 1 ]; then
                violated=$((violated + 1))
                curr=0
            fi
        fi
    done < "$FILE"

    rate=`echo "scale=2; $violated * 100 / $total" | bc`

    for t in ${LOG_DIR}/${CURR_RUN}/${bench}/*.txt; do
        isopredict -c -l $ISOLATION $t >> ${LOG_DIR}/${CURR_RUN}/${bench}/tmp
    done

    unserializable=`grep "unsat" ${LOG_DIR}/${CURR_RUN}/${bench}/tmp | wc -l`
    unser=`echo "scale=2; $unserializable * 100 / $total" | bc`

    echo "Benchmark: ${bench} Assertion Failure: ${rate}% Unserializable: ${unser}%"

    echo "----------------------------------------------"
    
}

LOG_DIR=isopredict_logs
CURR_RUN=`date +%Y-%m-%d-%H-%M-%S`
BENCH="smallbank,voter,tpcc,wikipedia"
ISOLATION=causal

[ -z $1 ] || BENCH=$1
[ -z $2 ] || ISOLATION=$2

mkdir -p $LOG_DIR
mkdir -p $LOG_DIR/$CURR_RUN/asserts

# bash build.sh
bash run.sh 30 $BENCH $ISOLATION 3 10

mv log/traces/* $LOG_DIR/$CURR_RUN/
mv log/monkeydb_asserts/*/*.out $LOG_DIR/$CURR_RUN/asserts
rm -rf log/monkeydb_asserts/*

for asserts in $LOG_DIR/$CURR_RUN/asserts/*.out; do
    check_assertions $asserts
done
