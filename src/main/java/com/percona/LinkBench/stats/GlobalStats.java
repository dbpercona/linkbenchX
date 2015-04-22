/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.percona.LinkBench.stats;


import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.ArrayList;
import java.util.Properties;

import java.lang.Math;

import org.apache.log4j.Logger;

import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.Config;
import com.facebook.LinkBench.LinkBenchOp;
import com.facebook.LinkBench.LinkStore;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import com.percona.LinkBench.stats.StatMessage;
import java.util.Collections;



/**
 * This class is used to keep track of statistics.  It collects a sample of the
 * total data (controlled by maxsamples) and can then compute statistics based
 * on that sample.
 *
 * The overall idea is to compute reasonably accurate statistics using bounded
 * space.
 *
 * Currently the class is used to print out stats at given intervals, with the
 * sample taken over a given time interval and printed at the end of the interval.
 */
public class GlobalStats implements Runnable  {

	private class LongArrayList extends ArrayList<Long>{}

  // Actual number of operations per type that caller did
  private long numops[];

  // samples for various optypes
  private ArrayList<Long>[] samples;

  // Concurrency metrcis
  private ArrayList<Integer> concArray;

  /** Number of operations that the sample is drawn from */
  private int opsSinceReset[];

  // minimums encounetered per operation type
  private long minimums[];

  // maximums encountered per operation type
  private long maximums[];

  // #errors encountered per type
  private long errors[];

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  /** Stream to write csv output to ( null if no csv output ) */
  private final PrintStream csvOutput;

  /** Random number generator used to decide which to include in sample */
  private Random rng;

  private Object lockQueue = new Object();

  /** Queue we use to get messages from Request thread */
  private BlockingQueue<StatMessage> statsQueue;

  /** time period to print current results */
  long displayFreq_ms;

  Properties props;

  public GlobalStats(BlockingQueue<StatMessage> statsQ, Properties props, PrintStream csvOutput) {

    this.csvOutput = csvOutput;
    this.statsQueue = statsQ;
    this.props = props;

    concArray = new ArrayList<Integer>();

    samples = new LongArrayList[LinkStore.MAX_OPTYPES];
    for (LinkBenchOp type: LinkBenchOp.values()) {
      samples[type.ordinal()] = new LongArrayList();
    }

/*
    samples = new long[LinkStore.MAX_OPTYPES][maxsamples];
    opsSinceReset = new int[LinkStore.MAX_OPTYPES];
    minimums = new long[LinkStore.MAX_OPTYPES];
    maximums = new long[LinkStore.MAX_OPTYPES];
    numops = new long[LinkStore.MAX_OPTYPES];
    errors = new long[LinkStore.MAX_OPTYPES];
*/
    rng = new Random();

    displayFreq_ms = ConfigUtil.getLong(props, Config.DISPLAY_FREQ, 60L) * 1000;

  }

  public void addStats(LinkBenchOp type, long timetaken, boolean error, int conc) {
	  synchronized(lockQueue) {
		  samples[type.ordinal()].add(timetaken);
		  concArray.add(conc);
	  }
  }


  public void resetSamples() {
    for (LinkBenchOp type: LinkBenchOp.values()) {
      opsSinceReset[type.ordinal()] = 0;
    }
  }
  /**
   * Write a header with column names for a csv file showing progress
   * @param out
   */
  public static void writeCSVHeader(PrintStream out) {
    out.println("timestamp,op,totalops,concurrency," +
            "max_us,p95_us,p99_us");
  }

  /**
   * @return total operation count so far for type
   */
  public long getCount(LinkBenchOp type) {
    return this.numops[type.ordinal()];
  }

  private void printStats() {

      long timestamp = System.currentTimeMillis() / 1000;

	  synchronized(lockQueue) {

		  int maxConc = 0;
		  if (concArray.size() > 0 ) {
			  Collections.sort(concArray);
			  maxConc = concArray.get(Math.max(concArray.size()*99/100,1)-1);
		  }
		  logger.info("Concurrency: count: " + concArray.size()+
				  " 99% concurrency: "+maxConc);
		  concArray.clear();

		  for (LinkBenchOp type: LinkBenchOp.values()) {
			  Collections.sort(samples[type.ordinal()]);
			  long maxTime=0;
			  long tm95th = 0;
			  long tm99th = 0;
			  int sz = samples[type.ordinal()].size();
			  if (sz > 0 ) {
				  maxTime = samples[type.ordinal()].get(sz-1);
				  tm95th = samples[type.ordinal()].get(Math.max(sz*95/100,1)-1);
				  tm99th = samples[type.ordinal()].get(Math.max(sz*99/100,1)-1);
			  }
			  if ( (type == LinkBenchOp.ADD_LINK) || (type == LinkBenchOp.GET_LINKS_LIST) ) {
				  logger.info("Type: " + type.name() + ", count: " + samples[type.ordinal()].size()+
						  ", conc: "+maxConc+", Time(us) max: "+maxTime+", 95th: "+ tm95th +", 99th: "+tm99th);
				  if (csvOutput != null) {
					  csvOutput.println(timestamp + "," + type.name() +
							  "," + samples[type.ordinal()].size() + "," + maxConc +
							  "," + maxTime + "," + tm95th +
							  "," + tm99th);
				  }
			  }
			  samples[type.ordinal()].clear();

			  //opsSinceReset[type.ordinal()] = 0;
		  }
	  }

  }

  @Override
  public void run() {
    logger.info("Global stats thread started");
    // logger.debug("Requester thread #" + requesterID + " first random number "
    //              + rng.nextLong());

    /* Start subthread that prints stats */ 
    Thread threadPrinter = new Thread("Printer Thread") {
	    public void run(){

		    try {	
			    while ( true ) {
				    Thread.sleep(displayFreq_ms);
					printStats();
				    //logger.info("Received message: " + timeRequest.type.displayName() + ", time: " 
			    }
		    } catch (InterruptedException e) {
				printStats();
				logger.info("Thread Interrupted");
		    }
	    }

    };

    threadPrinter.start();
    
	while ( true ) {
		// wait on incoming message
		try {
			StatMessage timeRequest = statsQueue.take();
			//                        long now = System.nanoTime();
			//                        long time = (now - t) / 1000; // Divide by 1000 to get result in microseconds
			if (timeRequest == null ) {
				continue;
			}
			addStats(timeRequest.type, timeRequest.execTime, false, timeRequest.concurrency);
			//logger.info("Received message: " + timeRequest.type.displayName() + ", time: " 
			// 		+ timeRequest.execTime);
		} catch (InterruptedException e) {
			logger.info("Thread Interrupted");
			break;
		}
	}
	threadPrinter.interrupt();
  }

}

