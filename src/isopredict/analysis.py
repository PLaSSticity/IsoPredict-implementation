import time
import random
from z3 import *
import isopredict.datastore as datastore
from isopredict.strategy import Strategy, Consistency

class Analysis(datastore.Symbolic):
    def __init__(self, db, bound=10, visualize=False, debug=False, strategy=Strategy.Full, consistency=Consistency.Causal, output="./out"):
        super().__init__(db, output)

        # configurations
        self.bound = self.db.transaction_count() if bound > db.transaction_count() else bound
        self.consistency = consistency
        self.debug = debug
        self.strategy = strategy
        self.visualize = visualize

        # statistics
        self.time_gencon = "N/A"
        self.time_solve = "N/A"

        # prediction boundary
        self.boundary = {session_id: Int("SessionBoundary[%s]"%session_id) for session_id in self.db.sessions.keys()}

        # predicted write-read choices
        self.choice = {k: {} for k in self.db.read_history.keys()}
        for k in self.choice.keys():
            self.choice[k] = {(read.transaction, read.seq): Int("Choice-T[%s]event[%s]key[%s]"%(read.transaction, read.seq, read.key)) for read in self.db.read_history[k]}

        # commit order from weak isolation level
        self.co_weak = None

    def event_boundary_constraints(self):
        constraints = []

        # session boundaries must be within the range of [0, number of events in that session]
        for session_id, transactions in self.db.sessions.items():
            boundary_candidates = [self.boundary[session_id] == self.db.session_event_count[session_id] + 1]
                
            if session_id in self.db.session_read_events:
                for seq in self.db.session_read_events[session_id]:
                    boundary_candidates.append(self.boundary[session_id] == seq + 1)

            constraints.append(Or(boundary_candidates))
        
        return And(constraints)

    def causal_consistency_constraints(self):
        co = self.create_commit_orders("Causal")
        # total_tx = self.db.transaction_count()
        constraints = [Distinct(list(co.values()))]

        self.co_weak = co

        # for c in co.values():
        #     constraints.append(And(c >= 0, c < total_tx))

        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

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

        # read committed axiom
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
                        wrk_t1_r1 = [If(self.event_on_boundary_constraint(r1),
                                 And(self.choice[r1.key][(r1.transaction, r1.seq)] == pos, self.event_in_boundary_constraint(self.db.write_history[k][pos])),
                                 And(self.event_in_boundary_constraint(r1), r1.write_tx == t1, self.event_in_boundary_constraint(self.db.write_history[k][pos]))) 
                                for pos in t1_writes]
                        
                        read_predecessors = [r for r in tx_reads[r1.transaction] if r.seq < r1.seq]
                        wr_t2_r2 = []
                        for r in read_predecessors:
                            if (t2, r.key) not in tx_writes:
                                continue

                            
                            wr_t2_r2 += [If(self.event_on_boundary_constraint(r),
                                     And(self.choice[r.key][(r.transaction, r.seq)] == pos, self.event_in_boundary_constraint(self.db.write_history[r.key][pos])),
                                     And(self.event_in_boundary_constraint(r), r.write_tx == t2, self.event_in_boundary_constraint(self.db.write_history[r.key][pos])))
                                    for pos in tx_writes[(t2, r.key)]]

                        constraints.append(Implies(And(Or(wrk_t1_r1), Or(wr_t2_r2)), 
                                                   co[t2] < co[t1]))
            
        # preserving write-read and session-order
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

                constraints.append(Implies(Or(self.wr(tx1, tx2), self.so(tx1, tx2)),
                                        co[tx1_id] < co[tx2_id]))

        return And(constraints)
    
    def unserializable_constraints_full(self):
        # commit order
        co = self.create_commit_orders("Serializable")
        # total_tx = self.db.transaction_count()

        # wwk constraints
        wwk_candidates = {k: self.prepare_wwk_candidates(k, co) for k in self.wwk.keys()}

        # ww constraints
        ww_candidates = {}

        # unserializability constraints
        constraints = [Distinct(list(co.values()))]

        # for c in co.values():
        #     constraints.append(And(c >= 0, c < total_tx))

        # prepare ww candidates
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

                ww = (tx1_id, tx2_id)

                for k in wwk_candidates:
                    if ww in wwk_candidates[k]:
                        if ww not in ww_candidates:
                            ww_candidates[ww] = []
                        ww_candidates[ww] += wwk_candidates[k][ww]

        # serializability constraint
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                if tx1_id == tx2_id:
                    continue

                # serializable arbitration over k
                ww = (tx1_id, tx2_id)
                if ww in ww_candidates:
                    # constraints.append(self.wwk[k](tx1, tx2) == Or(wwk_candidates[k][(tx1_id, tx2_id)]))
                    constraints.append(Implies(Or(self.wr(tx1, tx2), self.so(tx1, tx2), Or(ww_candidates[ww])),
                                                co[tx1_id] < co[tx2_id]))
                else:
                    # constraints.append(Not(self.wwk[k](tx1, tx2)))
                    constraints.append(Implies(Or(self.wr(tx1, tx2), self.so(tx1, tx2)),
                                                co[tx1_id] < co[tx2_id]))

        return Not(Exists(list(co.values()), And(constraints)))
    
    def unserializable_constraints_express(self):
        # constraints for unserializability
        constraints = []

        # total number of transactions
        total_tx = self.db.transaction_count()

        # wwk constraints
        wwk_candidates = {k: self.prepare_wwk_candidates_express(k) for k in self.wwk.keys()}

        # rwk constraints
        rwk_candidates = {k: self.prepare_rwk_candidates_express(k) for k in self.rwk.keys()}

        # reachability cycle constraints
        cycle = []

        # unserializability constraint
        for tx1_id, tx1 in self.tx.items():
            for tx2_id, tx2 in self.tx.items():
                # rank constraints
                constraints.append(And(self.rank(tx1, tx2) >= 0, self.rank(tx1, tx2) < total_tx * total_tx))

                if tx1_id == tx2_id:
                    constraints.append(Not(self.ww(tx1, tx2)))
                    constraints.append(Not(self.rw(tx1, tx2)))
                    constraints.append(Not(self.reachable(tx1, tx2)))
                    constraints.append(And(list(Not(wwx(tx1, tx2)) for wwx in self.wwk.values())))
                    constraints.append(And(list(Not(rwx(tx1, tx2)) for rwx in self.rwk.values())))
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

                # serializable antidependency over k
                for k in rwk_candidates:
                    if ww in rwk_candidates[k]:
                        constraints.append(self.rwk[k](tx1, tx2) == Or(rwk_candidates[k][ww]))
                    else:
                        constraints.append(Not(self.rwk[k](tx1, tx2)))

                # serializable antidependency
                constraints.append(self.rw(tx1, tx2) == Or(list(rwx(tx1, tx2) for rwx in self.rwk.values())))

                # reachability transitive closure
                rch_constraints = [self.hb(tx1, tx2), self.ar(tx1, tx2), self.ww(tx1, tx2), self.rw(tx1, tx2)]

                for tx3_id, tx3 in self.tx.items():
                    if tx3_id != tx1_id and tx3_id != tx2_id:
                        rch_constraints.append(And(self.reachable(tx1, tx3), 
                                                   self.rank(tx1, tx2) > self.rank(tx1, tx3),
                                                   self.rank(tx1, tx2) > self.rank(tx3, tx2),
                                                   Or(self.hb(tx3, tx2), self.ar(tx3, tx2), self.ww(tx3, tx2), self.rw(tx3, tx2))))
                        
                constraints.append(self.reachable(tx1, tx2) == Or(rch_constraints))

                # cycle constraint
                cycle.append(And(self.reachable(tx1, tx2), self.reachable(tx2, tx1)))
        
        # cycle exists
        constraints.append(Or(cycle))

        return And(constraints)
    
    def event_in_boundary_strict_constraint(self, event):
        return event.seq < self.boundary[event.session]
    
    def event_on_boundary_strict_constraint(self, event):
        return event.seq == self.boundary[event.session] - 1
    
    def event_in_boundary_constraint(self, event):
        if self.strategy == Strategy.Relaxed:
            return self.db.first_event_in_tx[event.transaction] < self.boundary[event.session]
        
        return self.event_in_boundary_strict_constraint(event)
    
    def event_on_boundary_constraint(self, event):
        if self.strategy == Strategy.Relaxed:
            lo = self.db.first_event_in_tx[event.transaction]
            hi = lo + self.db.transaction_event_count[event.transaction]

            return And(lo < self.boundary[event.session], hi >= self.boundary[event.session])
        
        return self.event_on_boundary_strict_constraint(event)
    
    def tx_in_boundary_constraint(self, tx_id):
        constraints = []

        for s_id, transactions in self.db.sessions.items():
            in_bound = [self.boundary[s_id] == self.db.session_event_count[s_id] + 1]
            for t_id in transactions:
                lo = self.db.first_event_in_tx[t_id]
                hi = lo + self.db.transaction_event_count[t_id]
                in_bound.append(And(lo < self.boundary[s_id], hi >= self.boundary[s_id], Not(self.hb(self.tx[t_id], self.tx[tx_id]))))
            
            constraints.append(Or(in_bound))

        return And(constraints)
        
    def prepare_so_candidates(self):
        # candidate tx pairs for so constraints
        so_candidates = {}

        # session id of transactions. key: transaction id   value: session_id
        session_of_tx = {}

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
                pos += self.db.transaction_event_count[tx_id]

                if i > 0:
                    prev_tx = transactions[i - 1]
                    so_candidates[(prev_tx, tx_id)] = True
        
        return so_candidates
    
    def prediction_choice_constraints(self):
        constraints = []

        for k, read_history in self.db.read_history.items():
            for r in read_history:
                c = self.choice[k][(r.transaction, r.seq)]
                choices = [And(c == i, self.event_in_boundary_constraint(self.db.write_history[k][i])) for i in range(len(self.db.write_history[k]))]
                # constraints.append(And(c >= 0, c < len(self.db.write_history[k])))

                constraints.append(Or(choices))

                for i in range(len(self.db.write_history[k])):
                    w = self.db.write_history[k][i]

                    # skip writes that come later in the same session
                    if w.session == r.session and w.seq > r.seq:
                        constraints.append(c != i)
                        continue

                    # skip writes that are too far away from og_write
                    # distance = self.db.transaction_distance(w.transaction, r.write_tx)
                    # if distance > self.bound:
                    #     constraints.append(c != i)
        
        return And(constraints)
    
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

        # go over the write history and read history
        for i in range(len(self.db.write_history[key])):
            write = self.db.write_history[key][i]

            for read in self.db.read_history[key]:
                if write.transaction == read.transaction:
                    continue

                wr = (write.transaction, read.transaction)
                if wr not in candidates:
                    candidates[wr] = []

                if write.transaction == read.write_tx and write.seq == read.write_seq and write.session == read.write_session:
                    candidates[wr].append(If(self.event_on_boundary_constraint(read),
                                            And(self.choice[key][(read.transaction, read.seq)] == i),
                                            And(self.event_in_boundary_constraint(read))))
                else:
                    candidates[wr].append(If(self.event_on_boundary_constraint(read),
                                            And(self.choice[key][(read.transaction, read.seq)] == i),
                                            False))
        
        return candidates
    
    def prepare_ark_candidates(self, key):
        ark = self.ark[key]
        wrk = self.wrk[key]

        # basic error checking
        if ark is None:
            return ark
        
        # candidate tx pair for ark constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates
                    
        for i in range(len(self.db.write_history[key])):
            write = self.db.write_history[key][i]

            for j in range(len(self.db.write_history[key])):
                if i == j:
                    continue

                conflict = self.db.write_history[key][j]

                ar = (conflict.transaction, write.transaction)
                if ar not in candidates:
                    candidates[ar] = []

                candidates[ar] += list(And(self.event_in_boundary_constraint(conflict),
                                        self.tx_in_boundary_constraint(conflict.transaction),
                                        self.tx_in_boundary_constraint(read.transaction),
                                        self.tx_in_boundary_constraint(write.transaction),
                                        wrk(self.tx[write.transaction], self.tx[read.transaction]),
                                        self.hb(self.tx[conflict.transaction], self.tx[read.transaction])) for read in self.db.read_history[key])
                
        return candidates
    
    def prepare_wwk_candidates(self, key, co):
        wwk = self.wwk[key]
        wrk = self.wrk[key]

        # basic error checking
        if wwk is None:
            return wwk
        
        # candidate tx pair for wwk constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates
                    
        for i in range(len(self.db.write_history[key])):
            write = self.db.write_history[key][i]

            for j in range(len(self.db.write_history[key])):
                if i == j:
                    continue

                conflict = self.db.write_history[key][j]

                ww = (conflict.transaction, write.transaction)
                if ww not in candidates:
                    candidates[ww] = []

                candidates[ww] += list(And(self.event_in_boundary_constraint(conflict),
                                        self.tx_in_boundary_constraint(conflict.transaction),
                                        self.tx_in_boundary_constraint(read.transaction),
                                        self.tx_in_boundary_constraint(write.transaction),
                                        wrk(self.tx[write.transaction], self.tx[read.transaction]),
                                        co[conflict.transaction] < co[read.transaction]) for read in self.db.read_history[key])
                
        return candidates
    
    def prepare_wwk_candidates_express(self, key):
        wwk = self.wwk[key]
        wrk = self.wrk[key]

        # basic error checking
        if wwk is None:
            return wwk
        
        # candidate tx pair for wwk constraints
        candidates = {}

        # skip keys that is read by none
        if key not in self.db.read_history:
            return candidates
                    
        for i in range(len(self.db.write_history[key])):
            write = self.db.write_history[key][i]

            for j in range(len(self.db.write_history[key])):
                if i == j:
                    continue

                conflict = self.db.write_history[key][j]

                ww = (conflict.transaction, write.transaction)
                if ww not in candidates:
                    candidates[ww] = []

                candidates[ww] += list(And(self.event_in_boundary_constraint(conflict),
                                        self.tx_in_boundary_constraint(conflict.transaction),
                                        self.tx_in_boundary_constraint(read.transaction),
                                        self.tx_in_boundary_constraint(write.transaction),
                                        wrk(self.tx[write.transaction], self.tx[read.transaction]),
                                        self.rank(self.tx[conflict.transaction], self.tx[write.transaction]) > self.rank(self.tx[conflict.transaction], self.tx[read.transaction]),
                                        self.reachable(self.tx[conflict.transaction], self.tx[read.transaction])) for read in self.db.read_history[key])
                
        return candidates
    
    def prepare_rwk_candidates_express(self, key):
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
            for conflict in self.db.write_history[key]:

                rw = (read.transaction, conflict.transaction)
                if rw not in candidates:
                    candidates[rw] = []

                candidates[rw] += list(And(self.event_in_boundary_constraint(conflict),
                                        self.tx_in_boundary_constraint(conflict.transaction),
                                        self.tx_in_boundary_constraint(read.transaction),
                                        self.tx_in_boundary_constraint(write.transaction),
                                        wrk(self.tx[write.transaction], self.tx[read.transaction]),
                                        self.rank(self.tx[read.transaction], self.tx[conflict.transaction]) > self.rank(self.tx[write.transaction], self.tx[conflict.transaction]),
                                        self.reachable(self.tx[write.transaction], self.tx[conflict.transaction])) for write in self.db.write_history[key])
                
        return candidates
    
    def do_prediction(self):
        s = Solver()
        s.set("timeout", 3600000 * 2)

        # start the clock
        start = time.perf_counter()

        # transaction constraints
        s.add(self.tx_constraints())

        # prediction boundary constraints
        s.add(self.event_boundary_constraints())

        # prediction choice constraints
        s.add(self.prediction_choice_constraints())

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
                    s.add(And(list(Not(wrx(tx1, tx2)) for wrx in self.wrk.values())))
                    s.add(And(list(Not(arx(tx1, tx2)) for arx in self.ark.values())))
                    continue
                
                # session order constraints
                if (tx1_id, tx2_id) in so_candidates:
                    s.add(self.so(tx1, tx2) == so_candidates[(tx1_id, tx2_id)])
                else:
                    s.add(Not(self.so(tx1, tx2)))

                # write-read over k constraints
                for k in wrk_candidates:
                    if (tx1_id, tx2_id) in wrk_candidates[k]:
                        s.add(self.wrk[k](tx1, tx2) == Or(wrk_candidates[k][(tx1_id, tx2_id)]))
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

        # constraints of a weak isolation level
        if self.consistency == Consistency.Causal:
            # causal consistency
            s.add(self.causal_consistency_constraints())
        elif self.consistency == Consistency.ReadCommitted:
            # read committed
            s.add(self.read_committed_constraints())

        # unserializability
        if self.strategy == Strategy.Full:
            s.add(self.unserializable_constraints_full())
        else:
            s.add(self.unserializable_constraints_express())

        # constraint generation time
        time_gencon = time.perf_counter()
        self.time_gencon = "%.3f"%(time_gencon - start)

        # solve
        res = s.check()

        # constraint solving time
        time_solve = time.perf_counter()
        self.time_solve = "%.3f"%(time_solve - time_gencon)

        print("Predictive: %s"%str(res))

        # print assertions
        if self.debug:
            self.print_assertions(s)

        if res != sat:
            return None, res
        
        return s.model(), res

    def predict(self):
        m, res = self.do_prediction()

        if m is None:
            return res
        
        if self.debug:
            self.print_model(m)

        if self.visualize:
            self.visualize_model(m)

        # prediction boundary of the history
        prediction_boundary = {}

        # boundary transactions
        boundary_tx = []

        # predicted write-read relations
        predicted_wr = []

        # output file for the predicted history
        out_file = "%s/unserializable_history_%s.txt"%(self.out, self.db.in_file)
        
        # print the borders
        print("Session Boundaries: ")
        for s_id, b in sorted(self.boundary.items()):
            prediction_boundary[s_id] = m.evaluate(b).as_long()
            print("Boundary of Session[%s] = %d/%d"%(s_id, prediction_boundary[s_id], self.db.session_event_count[s_id]))
            for t_id in self.db.sessions[s_id]:
                lo = self.db.first_event_in_tx[t_id]
                hi = lo + self.db.transaction_event_count[t_id]

                if lo < prediction_boundary[s_id] and hi >= prediction_boundary[s_id]:
                    boundary_tx.append(t_id)
                    break

        # concrete events from prediction
        tx_events = {}

        # predicted commit order of weak isolation level
        co_weak = sorted([(t_id, m.evaluate(cc).as_long()) for t_id, cc in self.co_weak.items()], key=lambda x: x[1])

        # observed commit order
        observed_co = sorted([(t, co) for t, co in self.db.observed_co.items()], key=lambda x: x[1])

        # predicted transaction commit order
        tx_order = []

        # calculate the predicted commit order
        # non-boundary transactions follow the observed commit order
        for t_id, i in observed_co:
            # exclude final state
            if t_id == datastore.FINAL_TX:
                continue

            # skip boundary transactions
            if t_id in boundary_tx:
                continue

            out_of_bound = False

            # exclude out-of-bound transactions
            for b_id in boundary_tx:
                hb = m.evaluate(self.hb(self.tx[b_id], self.tx[t_id]))
                if is_true(hb):
                    out_of_bound = True

            if out_of_bound:
                continue

            tx_order.append(t_id)
        
        # boundary transactions follow the weak isolation commit order
        for t_id, i in co_weak:
            # skip non-boundary transactions
            if t_id not in boundary_tx:
                continue

            tx_order.append(t_id)
            

        # add read events to tx_events
        for k, events in self.db.read_history.items():
            for r in events:
                # for reads that are beyond the prediction border, ignore them
                if self.strategy == Strategy.Relaxed:
                    if self.db.first_event_in_tx[r.transaction] >= prediction_boundary[r.session]:
                        continue
                else:
                    if r.seq >= prediction_boundary[r.session]:
                        continue

                # for reads that are not on the prediction border, they read from original writes
                if self.strategy == Strategy.Relaxed:
                    if self.db.first_event_in_tx[r.transaction] + self.db.transaction_event_count[r.transaction] < prediction_boundary[r.session]:
                        if r.transaction not in tx_events:
                            tx_events[r.transaction] = []
                        tx_events[r.transaction].append(r)
                        
                        continue
                else:
                    if r.seq < prediction_boundary[r.session] - 1:
                        if r.transaction not in tx_events:
                            tx_events[r.transaction] = []
                        tx_events[r.transaction].append(r)

                        continue

                # for reads that are on the prediction boundary, use predicted choices
                choice = m.evaluate(self.choice[k][(r.transaction, r.seq)]).as_long()
                write = self.db.write_history[k][choice]

                if self.db.first_event_in_tx[write.transaction] < prediction_boundary[write.session]:
                    if r.transaction not in tx_events:
                        tx_events[r.transaction] = []
                    tx_events[r.transaction].append(datastore.Read(r.session, r.transaction, r.seq, write.session, write.transaction, write.seq, k))

                # ignore write-read pairs that remained unchanged
                if write.transaction == r.write_tx:
                    continue

                predicted_wr.append((k, r, write, datastore.Write(r.write_session, r.write_tx, r.write_seq, r.key)))

        # add write events to tx_events
        for k, events in self.db.write_history.items():
            for e in events:
                # check whether event is out of bound
                if self.strategy == Strategy.Relaxed:
                    if self.db.first_event_in_tx[e.transaction] >= prediction_boundary[e.session]:
                        continue
                else:
                    if e.seq >= prediction_boundary[e.session]:
                        continue
                
                # add event to transactions
                if e.transaction not in tx_events:
                    tx_events[e.transaction] = []
                tx_events[e.transaction].append(e)
                
        # print events to file
        with open(out_file, "w") as out:
            for t_id in tx_order:
                if t_id not in tx_events:
                    continue

                # exclude final state
                if t_id == datastore.FINAL_TX:
                    continue

                out_of_bound = False
                
                # exclude out-of-bound transactions
                for b_id in boundary_tx:
                    hb = m.evaluate(self.hb(self.tx[b_id], self.tx[t_id]))
                    if is_true(hb):
                        out_of_bound = True

                if out_of_bound:
                    continue

                for e in sorted(tx_events[t_id], key=lambda ev: ev.seq):
                    out.write(str(e))

        # print write-read choices
        print("Predicted Read-Write pairs: ")
        if len(predicted_wr) == 0:
            print("The original execution was already unserializable. No need to change any write-read pairs\n")
        else:
            for (key, read, write, og_write) in predicted_wr:
                print("READ{Session[%s]Tx[%s]/%d}(%s) = WRITE{Session[%s]Tx[%s]/%d}, was previously reading from WRITE{Session[%s]Tx[%s]/%d}"%(
                    read.session, 
                    read.transaction, 
                    read.seq, 
                    key, 
                    write.session, 
                    write.transaction, 
                    write.seq,
                    og_write.session, 
                    og_write.transaction, 
                    og_write.seq
                ))

        return res
    
