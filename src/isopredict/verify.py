from z3 import *
import isopredict.datastore as datastore
from isopredict.strategy import Consistency

class Verifier(datastore.Symbolic):
    def __init__(self, db, visualize=False, debug=False, consistency=Consistency.Causal, output="./out"):
        super().__init__(db, output)

        # configurations
        self.consistency = consistency
        self.debug = debug
        self.visualize = visualize
        self.consistency = consistency

        # commit order
        self.co_weak = None
        self.co = None

    def prepare_so_candidates(self):
        # candidate tx pairs for so constraints
        so_candidates = {}

        # session id of transactions. key: transaction id   value: session_id
        session_of_tx = {}

        # transaction's position in its session (the position of its first event)   key: symbolic transaction id  value: int
        position_of_tx = {}

        # next_tx_in_session, session_of_tx and position_of_tx
        for session_id, transactions in self.db.sessions.items():
            # initial state
            so_candidates[(datastore.INIT_TX, transactions[0])] = True

            # # final state
            # so_candidates[(transactions[-1], datastore.FINAL_TX)] = True

            # regular session orders
            pos = 0
            for i in range(len(transactions)):
                tx_id = transactions[i]

                # session and position info
                session_of_tx[tx_id] = session_id
                position_of_tx[tx_id] = pos
                pos += self.db.transaction_event_count[tx_id]

                if i > 0:
                    prev_tx = transactions[i - 1]
                    so_candidates[(prev_tx, tx_id)] = True
        
        return so_candidates
    
    def prepare_wrk_candidates(self, key):
        wrk = self.wrk[key]

        # basic error checking
        if wrk is None:
            return wrk
        
        # candidate tx pairs for wrk constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates

        # go over the read history
        for r in self.db.read_history[key]:
            if r.write_tx == r.transaction:
                continue

            wr = (r.write_tx, r.transaction)
            if wr not in candidates:
                candidates[wr] = True
        
        return candidates
    
    def prepare_ark_candidates(self, key):
        ark = self.ark[key]

        # basic error checking
        if ark is None:
            return ark
        
        # candidate tx pair for ark constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates

        # go over the read history
        for r in self.db.read_history[key]:
            if r.write_tx == r.transaction:
                continue

            for conflict in self.db.write_history[key]:
                if conflict.transaction == r.write_tx:
                    continue

                ar = (conflict.transaction, r.write_tx)
                if ar not in candidates:
                    candidates[ar] = []
                candidates[ar].append(self.hb(self.tx[conflict.transaction], self.tx[r.transaction]))
                
        return candidates
    
    def prepare_rwk_candidates(self, key):
        rwk = self.rwk[key]
        wrk = self.wrk[key]

        # basic error checking
        if rwk is None:
            return rwk
        
        # candidate tx pair for rwk constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates
                    
        for read in self.db.read_history[key]:
            if read.transaction == read.write_tx:
                continue

            for conflict in self.db.write_history[key]:
                if conflict.transaction == read.write_tx:
                    continue

                rw = (read.transaction, conflict.transaction)
                if rw not in candidates:
                    candidates[rw] = []

                candidates[rw].append(self.hb(self.tx[read.write_tx], self.tx[conflict.transaction]))
                                    
                
        return candidates
    
    def causal_consistency_constraints(self):
        co = self.create_commit_orders("Causal")
        # total_tx = self.db.transaction_count()
        constraints = [Distinct(list(co.values()))]

        self.co_weak = co

        # for c in co.values():
        #     constraints.append(And(c >= 0, c < total_tx))

        # rwk constraints
        rwk_candidates = {k: self.prepare_rwk_candidates(k) for k in self.rwk.keys()}

        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

                rw = (tx1_id, tx2_id)

                # serializable antidependency over k
                for k in rwk_candidates:
                    if rw in rwk_candidates[k]:
                        constraints.append(self.rwk[k](tx1, tx2) == Or(rwk_candidates[k][rw]))
                    else:
                        constraints.append(Not(self.rwk[k](tx1, tx2)))

                # serializable antidependency
                constraints.append(self.rw(tx1, tx2) == Or(list(rwx(tx1, tx2) for rwx in self.rwk.values())))

                # Causal constraints
                constraints.append(Implies(Or(self.wr(tx1, tx2), self.so(tx1, tx2), self.ar(tx1, tx2)),
                                           co[tx1_id] < co[tx2_id]))

        return And(constraints)
    
    def read_committed_constraints(self):
        co = self.create_commit_orders("ReadCommitted")
        constraints = [Distinct(list(co.values()))]

        self.co_weak = co

        # read events of every transaction
        tx_reads = {}
        for k, read_history in self.db.read_history.items():
            for r in read_history:
                if r.transaction not in tx_reads:
                    tx_reads[r.transaction] = []
                tx_reads[r.transaction].append(r)
            
        # write events' positions inside write history
        tx_writes = {}
        for k, write_history in self.db.write_history.items():
            for i in range(len(write_history)):
                w = write_history[i]

                if (w.transaction, w.key) not in tx_writes:
                    tx_writes[(w.transaction, w.key)] = []

                tx_writes[(w.transaction, w.key)].append(i)

        # read commmitted axiom
        for k in self.db.write_history.keys():
            tx_writes_k = {}
            
            # skip writes that are not read
            if k not in self.db.read_history:
                continue

            # write events in every transaction that writes k
            for i in range(len(self.db.write_history[k])):
                w = self.db.write_history[k][i]
                if w.transaction not in tx_writes_k:
                    tx_writes_k[w.transaction] = []
                tx_writes_k[w.transaction].append(i)

            # for all pairs of trasactions t1, t2 that writes k
            for t1, t1_writes in tx_writes_k.items():
                for t2, t2_writes in tx_writes_k.items():
                    if t1 == t2:
                        continue
                    
                    # for all the events that reads k
                    for r1 in self.db.read_history[k]:
                        if r1.write_tx != t1:
                            continue
                        
                        read_predecessors = [r for r in tx_reads[r1.transaction] if r.seq < r1.seq]
                        wr_t2_r2 = []
                        for r in read_predecessors:
                            if (t2, r.key) not in tx_writes:
                                continue

                            wr_t2_r2 += [r.write_tx == t2]

                        constraints.append(Implies(Or(wr_t2_r2), 
                                                   co[t2] < co[t1]))
                        
        # rwk constraints
        rwk_candidates = {k: self.prepare_rwk_candidates(k) for k in self.rwk.keys()}
        
        # preserving write-read and session-order
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

                rw = (tx1_id, tx2_id)

                # serializable antidependency over k
                for k in rwk_candidates:
                    if rw in rwk_candidates[k]:
                        constraints.append(self.rwk[k](tx1, tx2) == Or(rwk_candidates[k][rw]))
                    else:
                        constraints.append(Not(self.rwk[k](tx1, tx2)))

                # serializable antidependency
                constraints.append(self.rw(tx1, tx2) == Or(list(rwx(tx1, tx2) for rwx in self.rwk.values())))

                # write-read and session-order
                constraints.append(Implies(Or(self.wr(tx1, tx2), self.so(tx1, tx2)),
                                           co[tx1_id] < co[tx2_id]))

        return And(constraints)
    
    def prepare_wwk_candidates(self, key, co):
        wwk = self.wwk[key]

        # basic error checking
        if wwk is None:
            return wwk
        
        # candidate tx pair for wwk constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates

        # go over the read history
        for r in self.db.read_history[key]:
            if r.write_tx == r.transaction:
                continue

            for conflict in self.db.write_history[key]:
                if conflict.transaction == r.write_tx:
                    continue

                ww = (conflict.transaction, r.write_tx)
                if ww not in candidates:
                    candidates[ww] = []
                candidates[ww].append(co[conflict.transaction] < co[r.transaction])
                
        return candidates
    
    def serializable_constraints(self):
        co = self.create_commit_orders("Serializable")
        # total_tx = self.db.transaction_count()

        self.co = co

        # wwk constraints
        wwk_candidates = {k: self.prepare_wwk_candidates(k, co) for k in self.wwk.keys()}

        constraints = [Distinct(list(co.values()))]

        # for c in co.values():
        #     constraints.append(And(c >= 0, c < total_tx))

        # generate ww constraints
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

                ww = (tx1_id, tx2_id)

                # serializable arbitration over k
                for k in wwk_candidates:
                    if ww in wwk_candidates[k]:
                        constraints.append(self.wwk[k](tx1, tx2) == Or(wwk_candidates[k][ww]))
                    else:
                        constraints.append(Not(self.wwk[k](tx1, tx2)))
                
                # serializable arbitration
                constraints.append(self.ww(tx1, tx2) == Or(list(wwx(tx1, tx2) for wwx in self.wwk.values())))

                # serializability
                constraints.append(Implies(Or(self.wr(tx1, tx2), self.so(tx1, tx2), self.ww(tx1, tx2)),
                                           co[tx1_id] < co[tx2_id]))

        return And(constraints)

    def do_check(self):
        s = Solver()
        s.set("timeout", 1800000)

        # transaction constraints
        s.add(self.tx_constraints())

        # prepare constraints
        # session order
        so_candidates = self.prepare_so_candidates()

        # wrk constraints
        wrk_candidates = {k: self.prepare_wrk_candidates(k) for k in self.wrk.keys()}

        # ark constraints
        ark_candidates = {k: self.prepare_ark_candidates(k) for k in self.ark.keys()}

        # so, wr, hb, ar constraints
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    s.add(Not(self.so(tx1, tx2)))
                    s.add(Not(self.wr(tx1, tx2)))
                    s.add(Not(self.hb(tx1, tx2)))
                    s.add(Not(self.ar(tx1, tx2)))
                    continue
                
                # session order constraints
                if (tx1_id, tx2_id) in so_candidates:
                    s.add(self.so(tx1, tx2))
                else:
                    s.add(Not(self.so(tx1, tx2)))

                # write-read over k constraints
                for k in wrk_candidates:
                    if (tx1_id, tx2_id) in wrk_candidates[k]:
                        s.add(self.wrk[k](tx1, tx2))
                    else:
                        s.add(Not(self.wrk[k](tx1, tx2)))

                # write-read
                s.add(self.wr(tx1, tx2) == Or(list(wrx(tx1, tx2) for wrx in self.wrk.values())))

                # happens-before transitive closure
                hb_constraints = [self.wr(tx1, tx2), self.so(tx1, tx2)]
                for tx3_id, tx3 in self.tx.items():
                    if tx3_id != tx1_id and tx3_id != tx2_id:
                        hb_constraints.append(And(self.hb(tx1, tx3), Or(self.wr(tx3, tx2), self.so(tx3, tx2))))
                s.add(self.hb(tx1, tx2) == Or(hb_constraints))

                # causal arbitration over k
                for k in ark_candidates:
                    if (tx1_id, tx2_id) in ark_candidates[k]:
                        s.add(self.ark[k](tx1, tx2) == Or(ark_candidates[k][(tx1_id, tx2_id)]))
                    else:
                        s.add(Not(self.ark[k](tx1, tx2)))

                # causal arbitration
                s.add(self.ar(tx1, tx2) == Or(list(arx(tx1, tx2) for arx in self.ark.values())))

        # visualization after an s.check() here
        res = s.check()
        if res != sat:
            print("Unexpected Error")
            # print assertions
            if self.debug:
                self.print_assertions(s)
                
            return None, res
        
        # basic model without weak isolation constraints
        m = s.model()

        # check weak consistency level
        if self.consistency == Consistency.Causal:
            s.add(self.causal_consistency_constraints())
        elif self.consistency == Consistency.ReadCommitted:
            s.add(self.read_committed_constraints())
        
        # verify consistency
        res = s.check()
        print("%s: %s"%(self.consistency, str(res)))

        if res != sat:
            # print assertions
            if self.debug:
                self.print_assertions(s)

            return m, res
        
        # update model with weak isolation constraints
        m = s.model()
        
        # check serializability
        s.add(self.serializable_constraints())

        # verify consistency
        res = s.check()
        print("Serializable: %s"%(str(res)))

        # print assertions
        if self.debug:
            self.print_assertions(s)

        if res != sat:
            return m, res
        
        return s.model(), res
    
    def verify(self):
        m, res = self.do_check()

        if m is not None and self.visualize:
            self.visualize_model(m)

        if self.debug:
            self.print_model(m)

        return res