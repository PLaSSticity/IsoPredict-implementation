TXCOUNT=4
LOG_DIR=isopredict_logs
CURR_RUN=`date +%Y-%m-%d-%H-%M-%S`
BENCH="smallbank,voter,tpcc,wikipedia"

[ -z $1 ] || TXCOUNT=$1
[ -z $2 ] || BENCH=$2

mkdir -p $LOG_DIR
mkdir -p $LOG_DIR/$CURR_RUN

# bash build.sh
bash run.sh 10 $BENCH causal 3 $TXCOUNT

mv log/traces/* $LOG_DIR/$CURR_RUN/

echo "Traces collection complete: $LOG_DIR/$CURR_RUN/"