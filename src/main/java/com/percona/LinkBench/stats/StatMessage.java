package com.percona.LinkBench.stats;

import com.facebook.LinkBench.LinkBenchOp;

public class StatMessage {
    public long execTime = 0;
    public int concurrency = 0;
    public LinkBenchOp type;

    public StatMessage(long eTime, int conc, LinkBenchOp optype) {
        execTime = eTime;
        concurrency = conc;
	type = optype;
    }
}
