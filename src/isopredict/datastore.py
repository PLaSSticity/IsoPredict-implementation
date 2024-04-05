import re
from z3 import *
from pathlib import Path
import isopredict.graph as graph

INIT_SESSION = "0"
INIT_TX = "0, 0"
FINAL_SESSION = "FIN"
FINAL_TX = "FIN, FIN"

class Event:
    def __init__(self, session_id, transaction_id, seq):
        self.session = session_id
        self.transaction = transaction_id
        self.seq = seq

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Event):
            return self.session == other.session and self.transaction == other.transaction and self.seq == other.seq
        return False

    def __ne__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, Event):
            return not (self.session == other.session and self.transaction == other.transaction and self.seq == other.seq)
        return False
    
    def __repr__(self):
        return "Session[%s]Tx[%s]Seq[%s]"%(self.session, self.transaction, self.seq)
    
    def __str__(self):
        return "Session[%s]Tx[%s]Seq[%s]"%(self.session, self.transaction, self.seq)
    
class Read(Event):
    def __init__(self, session_id, transaction_id, seq, write_session, write_tx, write_seq, key):
        super().__init__(session_id, transaction_id, seq)
        self.write_session = write_session
        self.write_tx = write_tx
        self.write_seq = write_seq
        self.key = key

    def __repr__(self):
        return "READ KEY[%s] Txn(%s) From(%s)\n"%(self.key, self.transaction, self.write_tx)
    
    def __str__(self):
        return "READ KEY[%s] Txn(%s) From(%s)\n"%(self.key, self.transaction, self.write_tx)
    
class Write(Event):
    def __init__(self, session_id, transaction_id, seq, key):
        super().__init__(session_id, transaction_id, seq)
        self.key = key

    def __repr__(self):
        return "WRITE KEY[%s] Txn(%s)\n"%(self.key, self.transaction)
    
    def __str__(self):
        return "WRITE KEY[%s] Txn(%s)\n"%(self.key, self.transaction)
    
class Symbolic:
    def __init__(self, db, output="out"):
        # execution trace
        self.db = db

        # output folder
        self.out = output
        Path(output).mkdir(parents=True, exist_ok=True)

        # symbolic transactions
        self.smtTx = self._create_tx_type()
        self.tx = self._init_tx()

        # SMT functions
        self.so = Function("Session-Order", self.smtTx, self.smtTx, BoolSort())
        self.wrk = {key: Function("Write-Read-%s"%key, self.smtTx, self.smtTx, BoolSort()) for key in self.db.write_history.keys()}
        self.wr = Function("Write-Read", self.smtTx, self.smtTx, BoolSort())
        self.hb = Function("Happens-Before", self.smtTx, self.smtTx, BoolSort())
        self.ark = {key: Function("Causal-Arbitration-%s"%key, self.smtTx, self.smtTx, BoolSort()) for key in self.db.write_history.keys()}
        self.ar = Function("Causal-Aribtration", self.smtTx, self.smtTx, BoolSort())
        self.wwk = {key: Function("Serial-Arbitration-%s"%key, self.smtTx, self.smtTx, BoolSort()) for key in self.db.write_history.keys()}
        self.ww = Function("Serial-Arbitration", self.smtTx, self.smtTx, BoolSort())
        self.rwk = {key: Function("Serial-Antidependency-%s"%key, self.smtTx, self.smtTx, BoolSort()) for key in self.db.write_history.keys()}
        self.rw = Function("Serial-AntiDependency", self.smtTx, self.smtTx, BoolSort())
        self.reachable = Function("Reachable", self.smtTx, self.smtTx, BoolSort())
        self.rank = Function("Rank", self.smtTx, self.smtTx, IntSort())

    def _create_tx_type(self):
        Transaction = Datatype("Transaction")

        # init transaction and regular transactions
        for session_id, transactions in self.db.sessions.items():
            for tx_id in transactions:
                Transaction.declare(tx_id)
        
        # # final transaction
        # Transaction.declare(FINAL_TX)

        # create the type
        Transaction = Transaction.create()
        return Transaction
    
    def _init_tx(self):
        tx = {}

        # initialize tx, session_of_tx and position_of_tx
        for session_id, transactions in self.db.sessions.items():
            for i in range(len(transactions)):
                tx_id = transactions[i]

                # symbolic transaction
                tx[tx_id] = getattr(self.smtTx, tx_id)

        # # final transaction
        # tx[FINAL_TX] = getattr(self.smtTx, FINAL_TX)
        
        return tx

    def tx_constraints(self):
        constraints = []
        constraints.append(Distinct(list(self.tx.values())))

        return And(constraints)
    
    def create_commit_orders(self, consistency):
        co = {}
        for session_id, transactions in self.db.sessions.items():
            for tx_id in transactions:
                co[tx_id] = Int("%s-CommitOrder[%s]"%(consistency, tx_id))
        
        # if FINAL_TX not in co:
        #     co[FINAL_TX] = Int("%s-CommitOrder[%s]"%(consistency, FINAL_TX))

        return co
    
    def print_assertions(self, s):
        out_file = "%s/assert_%s.txt"%(self.out, self.db.in_file)
        with open(out_file, "w") as out:
            for a in s.assertions():
                out.write("%s\n"%a)

    def print_model(self, m):
        if m is None:
            return
        
        out_file = "%s/model_%s.txt"%(self.out, self.db.in_file)
        with open(out_file, "w") as out:
            for (k, v) in sorted([(d, m[d]) for d in m], key = lambda x: str(x[0])):
                out.write("%s: %s\n"%(k, v))

    def visualize_model(self, m):
        # graph representation of the model
        g = graph.Graph("visualization_%s"%(self.db.in_file))

        # populate the graph with edges & nodes from the model
        for t_id, t in self.tx.items():
            for u_id, u in self.tx.items():
                if t_id == u_id:
                    continue

                src = "T[%s]"%(t_id)
                dst = "T[%s]"%(u_id)

                session_order = m.evaluate(self.so(t, u))
                write_read = m.evaluate(self.wr(t, u))
                causal_arb = m.evaluate(self.ar(t, u))
                write_write = m.evaluate(self.ww(t, u))
                read_write = m.evaluate(self.rw(t, u))

                if is_true(session_order):
                    g.add_edge(src, dst, "so")

                if is_true(write_read):
                    keys = []
                    for k, wrx in self.wrk.items():
                        if is_true(m.evaluate(wrx(t, u))):
                            keys.append(k)

                    g.add_edge(src, dst, "wr[%s]"%("\n".join(keys)))

                if is_true(causal_arb):
                    keys = []
                    for k, arx in self.ark.items():
                        if is_true(m.evaluate(arx(t, u))):
                            keys.append(k)

                    g.add_edge(src, dst, "ar[%s]"%("\n".join(keys)))

                if is_true(write_write):
                    keys = []
                    for k, wwx in self.wwk.items():
                        if is_true(m.evaluate(wwx(t, u))):
                            keys.append(k)

                    g.add_edge(src, dst, "ww[%s]"%("\n".join(keys)))

                if is_true(read_write):
                    keys = []
                    for k, rwx in self.rwk.items():
                        if is_true(m.evaluate(rwx(t, u))):
                            keys.append(k)

                    g.add_edge(src, dst, "rw[%s]"%("\n".join(keys)))

        g.visualize()


class DataStore:
    def __init__(self, in_file=""):
        self.sessions = {}   # key: session_id  value: list of transaction_id
        self.write_history = {}   # key: data_key  value: list of write Events that wrote to the same data_key
        self.read_history = {}    # key: data_key  value: list of read Events that read the key
        self.session_event_count = {}    # key: session_id  value: total number of events in the session
        self.first_event_in_tx = {}    # key: transaction_id  value: sequence number of first event in the transaction
        self.transaction_event_count = {}    # key: transaction_id  value: total number of events in the transaction
        self.session_read_events = {}    # key: session_id  value: list of that session's read events' sequence numbers
        self.observed_co = {}    # key: transaction_id  value: the order in which the transaction appeared in the observed exec
        self.in_file = in_file    # filename of the database log

    def find_write_seq(self, session_id, transaction_id, data_key):
        write_seq = -1

        # find write history
        if data_key not in self.write_history:
            return write_seq
        
        # find the most recent write on data_key from that particular session/transaction
        write_history = self.write_history[data_key]
        for event in write_history:
            if event.session == session_id and event.transaction == transaction_id and event.seq > write_seq:
                write_seq = event.seq
        
        return write_seq

    def add_read(self, session_id, tx_id, key, read_from_tx, read_from_session, ignore_po=False):
        # find the correct session
        if session_id not in self.sessions:
            self.sessions[session_id] = []
        session = self.sessions[session_id]

        # skip local reads
        local_write = self.find_write_seq(session_id, tx_id, key)
        if local_write >= 0:
            return

        # calculate observed commit order
        if tx_id not in self.observed_co:
            self.observed_co[tx_id] = len(self.observed_co.keys())
        
        # find the read history of current tx
        if key not in self.read_history:
            self.read_history[key] = []
        read_history = self.read_history[key]

        # find current number of events in session
        if session_id not in self.session_event_count:
            self.session_event_count[session_id] = 0
        seq = self.session_event_count[session_id]

        # for final transaction, there is no program order
        if ignore_po:
            seq = 0

        # add tx to session
        if tx_id not in session:
            session.append(tx_id)
            self.first_event_in_tx[tx_id] = seq

        # find current number of events in transaction
        if tx_id not in self.transaction_event_count:
            self.transaction_event_count[tx_id] = 0

        # find the sequence number of the write event
        write_seq = self.find_write_seq(read_from_session, read_from_tx, key)
        if write_seq < 0:
            # this only happens when reading from the initial state
            self.add_write(read_from_session, read_from_tx, key, True)
            write_seq = self.find_write_seq(read_from_session, read_from_tx, key)

        # append to the read history
        read_ev = Read(session_id, tx_id, seq, read_from_session, read_from_tx, write_seq, key)
        read_history.append(read_ev)

        # append sequence number to session_read_event list
        if session_id not in self.session_read_events:
            self.session_read_events[session_id] = []
        self.session_read_events[session_id].append(seq)

        # increment sequence count
        self.session_event_count[session_id] = seq + 1
        self.transaction_event_count[tx_id] += 1

    def remove_write(self, session_id, tx_id, key):
        if key not in self.write_history:
            return
        
        write_history = self.write_history[key]
        for event in write_history:
            if event.session == session_id and event.transaction == tx_id:
                write_history.remove(event)
        
        return

    def add_write(self, session_id, tx_id, key, init_tx=False):
        # find the correct session
        if session_id not in self.sessions:
            self.sessions[session_id] = []
        session = self.sessions[session_id]

        # calculate observed commit order
        if tx_id not in self.observed_co:
            self.observed_co[tx_id] = len(self.observed_co.keys())

        # find the write history of current key
        if key not in self.write_history:
            self.write_history[key] = []
        write_history = self.write_history[key]

        # find current number of events in session
        if session_id not in self.session_event_count:
            self.session_event_count[session_id] = 0
        seq = self.session_event_count[session_id]

        # add tx to session
        if tx_id not in session:
            session.append(tx_id)
            self.first_event_in_tx[tx_id] = seq

        # find current number of events in transaction
        if tx_id not in self.transaction_event_count:
            self.transaction_event_count[tx_id] = 0
        
        # remove existing write event
        self.remove_write(session_id, tx_id, key)

        # add tx to the write_history if it's not in there
        write_ev = Write(session_id, tx_id, seq, key)
        if write_ev not in write_history:
            if not init_tx:
                write_history.append(write_ev)
            else:
                write_history.insert(0, write_ev)

        # increment sequence count
        self.session_event_count[session_id] = seq + 1
        self.transaction_event_count[tx_id] += 1

    def add_initial_state(self):
        init_session = INIT_SESSION
        init_tx = INIT_TX

        for k in self.write_history.keys():
            write_seq = self.find_write_seq(init_session, init_tx, k)
            if write_seq < 0:
                self.add_write(init_session, init_tx, k, True)
    
    def add_final_state(self):
        for k, history in self.write_history.items():
            last_write = history[-1]
            self.add_read(FINAL_SESSION, FINAL_TX, k, last_write.transaction, last_write.session, True)

    def transaction_distance(self, tx1, tx2):
        if tx1 not in self.observed_co or tx2 not in self.observed_co:
            return -1
        
        distance = abs(self.observed_co[tx1] - self.observed_co[tx2])

        return distance

    def transaction_count(self):
        cnt = 0
        for session_id, transactions in self.sessions.items():
            if session_id == INIT_SESSION:
                continue
            cnt += len(transactions)
        return cnt
    
    def session_count(self):
        return len(self.sessions) - 1
    
    def event_count(self):
        return self.read_count() + self.write_count()
    
    def read_count(self):
        cnt = 0
        for history in self.read_history.values():
            cnt += len(history)

        return cnt
    
    def write_count(self):
        cnt = 0
        for history in self.write_history.values():
            for write in history:
                if write.session == INIT_SESSION:
                    continue
                cnt += 1
        
        return cnt
    
    def max_conflicting_write_count(self):
        cnt = 0
        for history in self.write_history.values():
            conflicts = len(history) - 2
            if conflicts > cnt:
                cnt = conflicts
        return cnt
    
    def conflicts_count(self):
        cnt = 0
        for history in self.write_history.values():
            if len(history) > 2:
                cnt += 1
        return cnt

    def read_only_tx_count(self):
        cnt = 0
        write_tx = set()
        for history in self.write_history.values():
            for write in history:
                write_tx.add(write.transaction)
        
        for transactions in self.sessions.values():
            for tx in transactions:
                if tx not in write_tx:
                    cnt += 1
        
        return cnt
    
    def write_only_tx_count(self):
        cnt = 0
        read_tx = set()
        for history in self.read_history.values():
            for read in history:
                read_tx.add(read.transaction)

        for transactions in self.sessions.values():
            for tx in transactions:
                if tx == INIT_TX:
                    continue
                if tx not in read_tx:
                    cnt += 1
        
        return cnt
    
    def conflicting_write_tx_count(self):
        conflicting_writes = set()
        for history in self.write_history.values():
            if len(history) <= 2:
                continue

            for write in history:
                if write.transaction == INIT_TX:
                    continue
                conflicting_writes.add(write.transaction)
        
        return len(conflicting_writes)
    
    def show_write_history(self):
        print("Conflicting Writes\n")

        for k, writes in self.write_history.items():
            if len(writes) > 1:
                print("%s: %d writes by %s"%(k, len(writes), writes))

    def show_tx_summary(self):
        print("Transaction Summary")

        read_cnt = {}
        write_cnt = {}

        for history in self.read_history.values():
            for read in history:
                if read.transaction not in read_cnt:
                    read_cnt[read.transaction] = 0
                read_cnt[read.transaction] += 1
            
        for history in self.write_history.values():
            for write in history:
                if write.transaction not in write_cnt:
                    write_cnt[write.transaction] = 0
                write_cnt[write.transaction] += 1
        
        # for session, transactions in self.sessions.items():
        #     if session == INIT_SESSION:
        #         continue

        #     for tx in transactions:
        #         reads = 0
        #         writes = 0

        #         if tx in read_cnt:
        #             reads = read_cnt[tx]
                
        #         if tx in write_cnt:
        #             writes = write_cnt[tx]

        #         print("Transaction [%s]: %d Reads, %d Writes"%(tx, reads, writes))

        observed_co = sorted(self.observed_co.items(), key=lambda t:t[1])
        for t in observed_co:
            t_id = t[0]
            reads = 0
            writes = 0

            if t_id == INIT_TX:
                continue

            if t_id in read_cnt:
                reads = read_cnt[t_id]

            if t_id in write_cnt:
                writes = write_cnt[t_id]
            
            print("Transaction [%s]: %d Reads, %d Writes"%(t_id, reads, writes))
    
    def show_stats(self):
        border = "-" * 24
        total_events = self.event_count()
        total_reads = self.read_count()
        total_writes = self.write_count()
        read_only_tx = self.read_only_tx_count()
        total_tx = self.transaction_count()
        total_sessions = self.session_count()

        print(border)
        print("Total Events: %d"%(total_events))
        print("Total Read Events: %d"%(total_reads))
        print("Total Write Events: %d"%(total_writes))
        print("Total Read-Only Transactions: %d"%(read_only_tx))
        print("Total Transactions: %d"%(total_tx))
        print("Total Sessions: %d"%(total_sessions))
        print(border)

        self.show_tx_summary()

        # self.show_write_history()
        # print(border)

def parse_log(filename):
    in_file = filename.split("/")[-1]
    in_file = in_file.split(".")[0]
    db = DataStore(in_file)
    line_num = 0

    with open(filename) as f:
        print("Parsing log file: %s"%filename)
        for line in f:
            line_num += 1

            read = re.search("KEY\[(.+?)\].*Txn\((.+?)\) From\((.+?)\)", line)
            if read:
                key = read.group(1)
                tx = read.group(2)
                read_from = read.group(3)

                # parse tx for session info
                tx_info = re.search("(.+?), (.+?)", tx)
                session = tx_info.group(1)
                seq = tx_info.group(2)

                # parse the read_from tx for session info
                tx_info_read_from = re.search("(.+?), (.+?)", read_from)
                read_from_session = tx_info_read_from.group(1)
                seq_read_from = tx_info_read_from.group(2)

                db.add_read(session, tx, key, read_from, read_from_session)

                # skip the remaining regex matching
                continue

            write = re.search("KEY\[(.+?)\] Txn\((.+?)\)", line)
            if write:
                key = write.group(1)
                tx = write.group(2)

                # parse tx for session info
                tx_info = re.search("(.+?), (.+?)", tx)
                session = tx_info.group(1)
                seq = tx_info.group(2)

                db.add_write(session, tx, key)

                # skip the remaining regex matching
                continue

            insert = re.search("INSERT\[(.+?)\] to Set\[(.+?)\] Txn\((.+?)\)", line)
            if insert:
                data_key = insert.group(1)
                set_key = insert.group(2)
                tx = insert.group(3)
                
                # parse tx for session info
                tx_info = re.search("(.+?), (.+?)", tx)
                session = tx_info.group(1)
                seq = tx_info.group(2)

                set_op = "Set(%s:%s)"%(set_key, data_key)

                db.add_write(session, tx, set_op)
                
                # skip the remaining regex matching
                continue

            contains = re.search("CONTAINS\[(.+?)\] in Set\[(.+?)\] From\((.+?)\) Txn\((.+?)\)", line)
            if contains:
                data_key = contains.group(1)
                set_key = contains.group(2)
                from_tx = contains.group(3)
                tx = contains.group(4)

                # parse tx for session info
                tx_info = re.search("(.+?), (.+?)", tx)
                session = tx_info.group(1)
                seq = tx_info.group(2)

                # parse the read_from tx for session info
                tx_info_read_from = re.search("(.+?), (.+?)", from_tx)
                read_from_session = tx_info_read_from.group(1)
                seq_read_from = tx_info_read_from.group(2)

                set_op = "Set(%s:%s)"%(set_key, data_key)
                
                db.add_read(session, tx, set_op, from_tx, read_from_session)

                # skip the remaining regex matching
                continue

            delete = re.search("DELETE\[(.+?)\] from Set\[(.+?)\] Txn\((.+?)\)", line)
            if delete:
                data_key = delete.group(1)
                set_key = delete.group(2)
                tx = delete.group(3)
                
                # parse tx for session info
                tx_info = re.search("(.+?), (.+?)", tx)
                session = tx_info.group(1)
                seq = tx_info.group(2)

                set_op = "Set(%s:%s)"%(set_key, data_key)

                db.add_write(session, tx, set_op)

                continue

    db.add_initial_state()
    # db.add_final_state()

    if db.transaction_count() == 0:
        print("[Error] trace was corrupted: %s"%filename)
        
        return None

    return db