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
package com.percona.LinkBench;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.facebook.LinkBench.Config;
import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkCount;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.NodeAutoIncrement;
import com.facebook.LinkBench.Phase;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkUpdateRequestBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteRequestBuilder;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteResult;

public class LinkStoreMongoDBv2 extends GraphStore {

  /* MongoDB database server configuration keys */
  public static final String CONFIG_HOST = "host";
  public static final String CONFIG_PORT = "port";
  public static final String CONFIG_USER = "user";
  public static final String CONFIG_PASSWORD = "password";
  public static final String CONFIG_BULK_INSERT_BATCH = "mongo_bulk_insert_batch";

  public static final int DEFAULT_BULKINSERT_SIZE = 1024;
  
  public static final int DEFAULT_RETRY = 5;
  public static final int DEFAULT_RETRY_MS = 500 ;

  private static final boolean INTERNAL_TESTING = false;

  String linktable;
  String counttable;
  String nodetable;
  String transtable;

  String host;
  String user;
  String pwd;
  String port;
  String defaultDB;

  Level debuglevel;

  private MongoClient mongoClient;
  private DB db;
  private DBCollection linkColl;
  private DBCollection countColl;
  private DBCollection nodeColl;
  private DBCollection transColl;

  private Phase phase;

  int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkStoreMongoDBv2() {
    super();
  }

  public LinkStoreMongoDBv2(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  public void initialize(Properties props, Phase currentPhase,
    int threadId) throws IOException, Exception {
    counttable = ConfigUtil.getPropertyRequired(props, Config.COUNT_TABLE);
    if (counttable.equals("")) {
      String msg = "Error! " + Config.COUNT_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    nodetable = props.getProperty(Config.NODE_TABLE);
    if (nodetable.equals("")) {
      // For now, don't assume that nodetable is provided
      String msg = "Error! " + Config.NODE_TABLE + " is empty!"
          + "Please check configuration file.";
      logger.error(msg);
      throw new RuntimeException(msg);
    }

    host = ConfigUtil.getPropertyRequired(props, CONFIG_HOST);
    user = ConfigUtil.getPropertyRequired(props, CONFIG_USER);
    pwd = ConfigUtil.getPropertyRequired(props, CONFIG_PASSWORD);
    port = props.getProperty(CONFIG_PORT);
    defaultDB = ConfigUtil.getPropertyRequired(props, Config.DBID);

    if (port == null || port.equals("")) port = "3306"; //use default port
    debuglevel = ConfigUtil.getDebugLevel(props);
    phase = currentPhase;

    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_BATCH);
    }

    linktable = ConfigUtil.getPropertyRequired(props, Config.LINK_TABLE);
    transtable = ConfigUtil.getPropertyRequired(props, Config.TRANS_TABLE);
    
    // connect
    try {
      openConnection();
    } catch (Exception e) {
      logger.error("error connecting to database:", e);
      throw e;
    }
    
    // initialize node id sequence - This is necessary as MongoDB does
    // not support integer sequences
    synchronized (LinkStoreMongoDBv2.class) {
      if (NodeAutoIncrement.getInstance().getLastSequence() == 0) {
        long lastId=0;
        // get the max node id
        DBCursor nodeCurr = nodeColl.
            find().
            sort(new BasicDBObject("id",-1)).
            limit(1);
        if (nodeCurr.hasNext()) {
          lastId=(Long)nodeCurr.next().get("id");
        }
        NodeAutoIncrement.getInstance().setNext(lastId+1);
      }
    }
    
  }

  // connects to test database
  @SuppressWarnings("serial")
  private void openConnection() throws Exception {
    db = null;
    Random rng = new Random();

    StringBuilder mongoUri = new StringBuilder("mongodb://");
    if (user != null && !"".equals(user.trim())
        && pwd != null && !"".equals(pwd.trim())) {
      mongoUri.append(user+":"+pwd+"@");
    }
    if (host != null && !"".equals(host.trim()))
      mongoUri.append(host);
    if (port != null && !"".equals(port.trim()))
      mongoUri.append(":"+port);
    mongoUri.append("/");
    if (defaultDB != null && !"".equals(defaultDB.trim()))
      mongoUri.append(defaultDB);
    
    /* Fix for failing connections at high concurrency, 
     * short random delay for each */
    try {
      int t = rng.nextInt(1000) + 100;
      //System.err.println("Sleeping " + t + " msecs");
      Thread.sleep(t);
    } catch (InterruptedException ie) {
    }

    mongoClient = new MongoClient(
        new MongoClientURI( mongoUri.toString() )
    );
    
    db = mongoClient.getDB( defaultDB );
    
    try {
      int t = rng.nextInt(1000) + 100;
      //System.err.println("Sleeping " + t + " msecs");
      Thread.sleep(t);
    } catch (InterruptedException ie) {
    }

    // get handles to the collections
    linkColl = getOrCreateCollection(linktable);
    nodeColl = getOrCreateCollection(nodetable);
    countColl = getOrCreateCollection(counttable);
    transColl = getOrCreateCollection(transtable);
    
    // our collections
    if (phase == Phase.LOAD) {
      
      // create indexes
      linkColl.createIndex(
          new BasicDBObject(new LinkedHashMap<String,Object>(){{
            put("link_type",new Integer("1"));
            put("id1",new Integer("1"));
            put("id2",new Integer("1"));
          }}),
          new BasicDBObject("unique",true)
      );
      linkColl.createIndex(
          new BasicDBObject(new LinkedHashMap<String,Object>(){{
            put("id1",new Integer("1"));
            put("link_type",new Integer("1"));
            put("visibility",new Integer("1"));
            put("time",new Integer("1"));
            put("id2",new Integer("1"));
            put("version",new Integer("1"));
            put("data",new Integer("1"));
          }}),
          new BasicDBObject("unique",true)
      );
      countColl.createIndex(
          new BasicDBObject(new LinkedHashMap<String,Object>(){{
            put("id",new Integer("1"));
            put("link_type",new Integer("1"));
          }}),
          new BasicDBObject("unique",true)
      );
      nodeColl.createIndex(
          new BasicDBObject(new LinkedHashMap<String,Object>(){{
            put("id",new Integer("1"));
          }}),
          new BasicDBObject("unique",true)
      );
      transColl.createIndex(
          new BasicDBObject(new LinkedHashMap<String,Object>(){{
            put("link_type",new Integer("1"));
            put("id1",new Integer("1"));
            put("id2",new Integer("1"));
          }})
      );
      
    }

  }

  public DBCollection getOrCreateCollection(String collectionName) {
    boolean collectionExists = db.collectionExists(collectionName);
    if (collectionExists == false) {
        db.createCollection(collectionName, null);
    }
    return(db.getCollection(collectionName));
  }
  
  @Override
  public void close() {
    try {
      mongoClient.close();
    } catch (MongoClientException mce) {
      logger.error("Error while closing MongoDB connection: ", mce);
    }
  }

  public void clearErrors(int threadID) {
    logger.info("Reopening MongoDB connection in threadID " + threadID);

    try {
      if (mongoClient != null) {
        mongoClient.close();
      }
      openConnection();
    } catch (Throwable e) {
      e.printStackTrace();
      return;
    }
  }

  /**
   * Handle SQL exception by logging error and selecting how to respond
   * @param ex SQLException thrown by MySQL JDBC driver
   * @return true if transaction should be retried
   */
  private boolean processMongoDBException(MongoClientException ex, String op) {
    boolean retry=false;
    // do we need to implement retries on any exceptions?
    String msg = "MongoDBException thrown by MongoDB driver during execution of " +
                 "operation: " + op + ".  ";
    msg += "Message was: '" + ex.getMessage() + "'.  ";
    msg += "Code was: " + ex.getCode() + ".  ";

    if (retry) {
      msg += "Error is probably transient, retrying operation.";
      logger.warn(msg);
    } else {
      msg += "Error is probably non-transient, will abort operation.";
      logger.error(msg);
    }
    return retry;
  }

  // get count for testing purpose
  private void testCount(DBCollection assocColl, DBCollection cntColl,
                         long id, long link_type)
    throws Exception {

    // count from link/node collection
    BasicDBObject query1 = new BasicDBObject();
    query1.put("id1", id);
    query1.put("link_type", link_type);
    query1.put("visibility", LinkStore.VISIBILITY_DEFAULT);
    int aCount = assocColl.find(query1).count();

    // sum from count collection
    BasicDBObject match2 = new BasicDBObject();
    match2.put("id1", id);
    match2.put("link_type", link_type);
    
    BasicDBObject group2 = new BasicDBObject();
    group2.put("_id", null);
    group2.put("total", new BasicDBObject("$sum","count"));

    ArrayList<DBObject> list2=new ArrayList<DBObject>();
    list2.add(new BasicDBObject("$match", match2));
    list2.add(new BasicDBObject("$group", group2));

    AggregationOutput cntAgg = cntColl.aggregate(list2);
    
    // get the total count
    int cntSum=0;
    if (cntAgg != null) {
      Iterable<DBObject> r=cntAgg.results();
      if (r != null && r.iterator().hasNext()) {
        cntSum=Integer.parseInt(r.iterator().next().get("total").toString()); 
      }
    }
    
    int ret = aCount == cntSum ? 1 : 0;

    if (ret != 1) {
      throw new Exception("Data inconsistency between " + assocColl.getName() +
                          " and " + cntColl.getName());
    }    
    
  }

  @Override
  public boolean addLink(String dbid, Link l, boolean noinverse)
    throws Exception {
    while (true) {
      try {
        return addLinkImpl(l, noinverse);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "addLink")) {
          throw ex;
        }
      }
    }
  }

  private boolean addLinkImpl(Link l, boolean noinverse)
      throws Exception {

     if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("addLink " + l.id1 +
                         "." + l.id2 +
                         "." + l.link_type);
    }

    // if the link is already there then update its visibility
    // only update visibility; skip updating time, version, etc.

    int nrows = addLinksNoCount(Collections.singletonList(l));

    // Note: at this point, we have an exclusive lock on the link
    // row until the end of the transaction, so can safely do
    // further updates without concurrency issues.

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("nrows = " + nrows);
    }

    // based on nrows, determine whether the previous query was an insert
    // or update
    boolean row_found;
    boolean update_data = false;
    int update_count = 0;

    switch (nrows) {
      case 1:
        // a new row was inserted --> need to update counttable
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        }
        row_found = false;
        break;

      case 0:
        // A row is found but its visibility was unchanged
        // --> need to update other data
        update_data = true;
        row_found = true;
        break;

      case 2:
        // a visibility was changed from VISIBILITY_HIDDEN to DEFAULT
        // or vice-versa
        // --> need to update both counttable and other data
        if (l.visibility == VISIBILITY_DEFAULT) {
          update_count = 1;
        } else {
          update_count = -1;
        }
        update_data = true;
        row_found = true;
        break;

      default:
        String msg = "Value of affected-rows number is not valid" + nrows;
        logger.error("MongoDB Error: " + msg);
        throw new Exception(msg);
    }

    // MongoDB Perform Two Phase Commits
    // since MongoDB does not ACID compliant, this two-phase commit 
    // transaction methodology is recommend in the MongoDB docs 
    // http://docs.mongodb.org/manual/tutorial/perform-two-phase-commits

    // start a transaction
    
    BasicDBObject transObj=new BasicDBObject();
    transObj.put("id1",l.id1);
    transObj.put("id2",l.id2);
    transObj.put("link_type",l.link_type);
    transObj.put("update_count", update_count);
    transColl.save(transObj);
    DBObject transInit=transColl.findOne(transObj);
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("trans init:"+transObj.toString());
    }

    // set transaction to pending

    if (transInit != null) {
      BasicDBObject tid=new BasicDBObject("_id",transInit.get("_id"));
      BasicDBObject tstate=new BasicDBObject();
      tstate.put("$set",new BasicDBObject("state","pending"));
      transColl.update(tid, tstate);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("trans pending:"+tid.toString());
      }
    }
    
    if (update_count != 0) {
      int base_count = update_count < 0 ? 0 : 1;
      // query to update counttable
      // if (id, link_type) is not there yet, add a new record with count = 1
      // The update happens atomically, with the latest count and version
      long currentTime = (new Date()).getTime();

      // again, we cannot upsert as the count increment 
      // logic is different between insert and update
      
      BasicDBObject countKey=new BasicDBObject();
      countKey.put("id",l.id1);
      countKey.put("link_type",l.link_type);
      countKey.put("pendingTransactions",
          new BasicDBObject("$ne",transInit.get("_id"))
      );
      
      BasicDBObject countObj=new BasicDBObject();
      countObj.put("id",l.id1);
      countObj.put("link_type",l.link_type);
      countObj.put("time", currentTime);

      // upsert
      WriteResult countResult = countColl.update(countKey, countObj,true, false);

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("count upsert:"+countKey+","+countObj);
      }

      // increment count and version
      BasicDBObject countInc=new BasicDBObject();
      if (countResult.isUpdateOfExisting()) {
        // was update
        countInc.put("$inc",new BasicDBObject("count",update_count));
        countInc.put("$inc",new BasicDBObject("version",1));
      } else {
        // was insert
        countInc.put("$inc",new BasicDBObject("count",base_count));
        countInc.put("$inc",new BasicDBObject("version",0));
      }
      countInc.put("$push",
          new BasicDBObject("pendingTransactions",transInit.get("_id"))
      );
      countColl.update(countKey, countInc);

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("count inc:"+countKey+","+countInc);
      }
      
    }
      
    if (update_data) {

      BasicDBObject linkKey=new BasicDBObject();
      linkKey.put("link_type", l.link_type);
      linkKey.put("id1", l.id1 );
      linkKey.put("id2", l.id2 );
      linkKey.put("pendingTransactions",
          new BasicDBObject("$ne",transInit.get("_id"))
      );
      
      BasicDBObject linkObj=new BasicDBObject();
      linkObj.put("visibility", l.visibility);
      linkObj.put("data", l.data);
      linkObj.put("time", l.time);
      linkObj.put("version", l.version);
      linkColl.update(linkKey,
          new BasicDBObject("$set",linkObj)
      );
      
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("update:"+linkObj.toString());
      }

      BasicDBObject linkTrans=new BasicDBObject();
      linkTrans.put("$push",
          new BasicDBObject("pendingTransactions",transInit.get("_id"))
      );
      linkColl.update(linkKey, linkTrans);

      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("link pendingTransaction:"+linkObj.toString());
      }
      
    }
    
    // mark transaction as committed
    if (transInit != null) {
      BasicDBObject tid=new BasicDBObject("_id",transInit.get("_id"));
      BasicDBObject tstate=new BasicDBObject();
      tstate.put("$set",new BasicDBObject("state","committed"));
      transColl.update(tid, tstate);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("trans committed:"+tid.toString());
      }

      // remove pending transaction from count
      BasicDBObject countKey=new BasicDBObject();
      countKey.put("id",l.id1);
      countKey.put("link_type",l.link_type);
      BasicDBObject countPull=new BasicDBObject();
      countPull.put("$pull",
          new BasicDBObject("pendingTransactions",transInit.get("_id"))
      );
      countColl.update(countKey,countPull);
      
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("count pull trans:"+countKey.toString()+","+countPull.toString());
      }
      
      // remove pending transaction from link
      BasicDBObject linkKey=new BasicDBObject();
      linkKey.put("link_type", l.link_type);
      linkKey.put("id1", l.id1 );
      linkKey.put("id2", l.id2 );
      // 
      BasicDBObject linkObj=new BasicDBObject();
      linkObj.put("$pull",
          new BasicDBObject("pendingTransactions",transInit.get("_id"))
      );
      linkColl.update(linkKey, linkObj);
      
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("link pull trans:"+countKey.toString()+","+countPull.toString());
      }

      // Mark transaction as done
      tstate.put("$set",new BasicDBObject("state","done"));
      transColl.update(tid, tstate);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("trans done:"+tid.toString());
      }
      
    }
    
    if (INTERNAL_TESTING) {
      testCount(linkColl, countColl, l.id1, l.link_type);
    }
    return row_found;
  }

  /**
   * Internal method: add links without updating the count
   * @param dbid
   * @param links
   * @return
   * @throws SQLException
   */
  private int addLinksNoCount(List<Link> links)
      throws MongoClientException {
    if (links.size() == 0)
      return 0;

    int nrows=0;
    
    BulkWriteOperation bulkWriteOperation = linkColl.initializeUnorderedBulkOperation();
    
    for (Link l : links) {
      
      // find by primary key
      BasicDBObject linkKey=new BasicDBObject();
      linkKey.put("link_type", l.link_type);
      linkKey.put("id1",l.id1);
      linkKey.put("id2", l.id2);
//      linkKey.put("visibility", new BasicDBObject(
//          "$ne", l.visibility
//      ));
      
      BulkWriteRequestBuilder bulkWriteRequestBuilder =
          bulkWriteOperation.find(linkKey);

      // upsert a link
      BasicDBObject linkIns=new BasicDBObject();
      linkIns.put("id1",l.id1);
      linkIns.put("id2",l.id2);
      linkIns.put("link_type",l.link_type);
      linkIns.put("data",l.data);
      linkIns.put("time",l.time);
      linkIns.put("version",l.version);
      
      BasicDBObject linkUpd=new BasicDBObject();
      linkUpd.put("visibility",l.visibility);
      
      BasicDBObject linkUpsert=new BasicDBObject();
      linkUpsert.put("$setOnInsert",linkIns);
      linkUpsert.put("$set",linkUpd);
      
      BulkUpdateRequestBuilder upsertReq=bulkWriteRequestBuilder.upsert();
      upsertReq.update(linkUpsert);
      
    }
    
    BulkWriteResult result = bulkWriteOperation.execute(); 
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("bulk links no count:"+result);
    }

    // when there is only one object in the list
    // then this should return a 1 for INSERT
    // and a 2 for UPDATE similar to MySQL
    // INSERT ... ON DUPLICATE KEY UPDATE ...
    
    nrows += result.getInsertedCount();
    nrows += (result.getModifiedCount() * 2);
    return nrows;

  }
  
  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                         boolean noinverse, boolean expunge)
    throws Exception {
    while (true) {
      try {
        return deleteLinkImpl(dbid, id1, link_type, id2, noinverse, expunge);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "deleteLink")) {
          throw ex;
        }
      }
    }
  }

  private boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("deleteLink " + id1 +
                         "." + id2 +
                         "." + link_type);
    }

    // First do a select to check if the link is not there, is there and
    // hidden, or is there and visible;
    // Result could be either NULL, VISIBILITY_HIDDEN or VISIBILITY_DEFAULT.
    // In case of VISIBILITY_DEFAULT, later we need to mark the link as
    // hidden, and update counttable.
    // We lock the row exclusively because we rely on getting the correct
    // value of visible to maintain link counts.  Without the lock,
    // a concurrent transaction could also see the link as visible and
    // we would double-decrement the link count.
    
    // get link and make sure we are not in the middle of a 
    // transaction
    //
    BasicDBObject linkKey=new BasicDBObject();
    linkKey.put("link_type", link_type);
    linkKey.put("id1", id1);
    linkKey.put("id2", id2);
    linkKey.put("pendingTransactions", new BasicDBObject(
                  "$eq", new BasicDBObject(
                      "$size", 0
        )
    ));

    // start a transaction
    BasicDBObject transObj=new BasicDBObject();
    transObj.put("id1",id1);
    transObj.put("id2",id2);
    transObj.put("link_type",link_type);
    transObj.put("update_count", -1);
    transObj.put("state","initial");
    transColl.save(transObj);
    DBObject transInit=transColl.findOne(transObj);
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("trans init:"+transObj.toString());
    }

    // set transaction to pending
    BasicDBObject tid=null;
    BasicDBObject tstate=null;
    if (transInit != null) {
      tid=new BasicDBObject("_id",transInit.get("_id"));
      tstate=new BasicDBObject();
      tstate.put("$set",new BasicDBObject("state","pending"));
      transColl.update(tid, tstate);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("trans pending:"+tid.toString());
      }
    }

    // retrieve the link
    DBObject linkObj=linkColl.findOne(linkKey);
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("findOne:"+linkKey+","+linkObj);
    }

    int visibility = -1;
    boolean found = false;
    if (linkObj != null) {
      visibility = ((Integer)linkObj.get("visibility")).intValue();
      found = true;
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace(String.format("(%d, %d, %d) visibility = %d",
                id1, link_type, id2, visibility));
    }

    ObjectId countId=null;  // id of count record to be updated
    
    if (!found) {
      // do nothing
    }
    else if (visibility == VISIBILITY_HIDDEN && !expunge) {
      // do nothing
    }
    else {
      
      BasicDBObject linkId=new BasicDBObject("_id",linkObj.get("_id"));

      if (!expunge) {
        
        // add pending transaction to link
        BasicDBObject linkUpd = new BasicDBObject();
        linkUpd.put("$push",
            new BasicDBObject("pendingTransactions",transInit.get("_id"))
        );
        linkUpd.put("$set",
            new BasicDBObject("visibility",VISIBILITY_HIDDEN)
        );
        linkColl.update(linkId, linkUpd);
        
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("update visibility: "+linkKey);
        }
        
      } else {
        linkColl.remove(linkId);
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("remove: "+linkKey);
        }
      }
      
      // update count table
      // * if found (id1, link_type) in count table, set
      //   count = (count == 1) ? 0) we decrease the value of count
      //   column by 1;
      // * otherwise, insert new link with count column = 0
      // The update happens atomically, with the latest count and version
      long currentTime = (new Date()).getTime();

      BasicDBObject countKey=new BasicDBObject();
      countKey.put("id", id1);
      countKey.put("link_type",link_type);

      BasicDBObject countObj=new BasicDBObject();
      countObj.put("id",id1);
      countObj.put("link_type",link_type);
      countObj.put("time",currentTime);

      WriteResult countRes = countColl.update(countKey, countObj, true, false);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("count upsert:"+countRes);
      }
      
      boolean update=false;
      BasicDBObject countMod=new BasicDBObject();
      if (countRes.isUpdateOfExisting()) {
        // update
        countMod.put("$inc",new BasicDBObject("count",-1));
        countMod.put("$inc",new BasicDBObject("version",1));
        update=true;
      } else {
        // insert
        countMod.put("$inc",new BasicDBObject("count",0));
        countMod.put("$inc",new BasicDBObject("version",0));
      }
      WriteResult countDec=countColl.update(countKey, countMod);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("count dec:"+countDec);
      }
      // fix up for negative counts
      if (update) {
        countKey.put("count",new BasicDBObject("$lt",0));
        WriteResult countFix=countColl.update(countKey, new BasicDBObject("$set",
            new BasicDBObject("count",0))
        );
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("update:"+countFix);
        }
      }
      
      DBObject countCurr=countColl.findOne(countKey);
      if (countCurr == null) {
        // insert
        countObj.put("count",0);
        countColl.insert(countObj);
        countId=countObj.getObjectId("_id");
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("insert:"+countObj);
        }
      } else {
        // update
        int count=0;
        try {
          Integer.parseInt(countCurr.get("count").toString());
        } catch (Exception e) {} // just leave it zero
        countCurr.put("count", count == 0 ? count : count - 1);
        countColl.update(countKey, countCurr);
        countId=(ObjectId)countObj.get("_id");
      }
      countObj.put("$push",
          new BasicDBObject("pendingTransactions",transInit.get("_id"))
      );
      
    }
    
    // mark transaction as committed
    if (transInit != null) {
      tstate.put("$set",new BasicDBObject("state","committed"));
      transColl.update(tid, tstate);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("trans committed:"+tid.toString());
      }

      // pull committed transaction
      BasicDBObject transPull=new BasicDBObject();
      transPull.put("$pull",
          new BasicDBObject("pendingTransactions",transInit.get("_id"))
      );

      // remove from link (if it exists)
      linkColl.update(linkKey,transPull);
      // remove from count (if it exists)
      if (countId != null) {
        countColl.update(new BasicDBObject("_id",countId.toString()),transPull);
        if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
          logger.trace("trans pull:"+countId.toString());
        }
      }

      // Mark transaction as done
      tstate.put("$set",new BasicDBObject("state","done"));
      transColl.update(tid, tstate);
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("trans done:"+tid.toString());
      }
    }
    
    // set transaction 
    if (INTERNAL_TESTING) {
      testCount(linkColl, countColl, id1, link_type);
    }

    return found;
  }

  @Override
  public boolean updateLink(String dbid, Link l, boolean noinverse)
    throws Exception {
    // Retry logic is in addLink
    boolean added = addLink(dbid, l, noinverse);
    return !added; // return true if updated instead of added
  }


  // lookup using id1, type, id2
  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
    throws Exception {
    while (true) {
      try {
        return getLinkImpl(dbid, id1, link_type, id2);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "getLink")) {
          throw ex;
        }
      }
    }
  }

  private Link getLinkImpl(String dbid, long id1, long link_type, long id2)
    throws Exception {
    Link res[] = multigetLinks(dbid, id1, link_type, new long[] {id2});
    if (res == null) return null;
    assert(res.length <= 1);
    return res.length == 0 ? null : res[0];
  }


  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
                              long[] id2s) throws Exception {
    while (true) {
      try {
        return multigetLinksImpl(dbid, id1, link_type, id2s);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "multigetLinks")) {
          throw ex;
        }
      }
    }
  }

  private Link[] multigetLinksImpl(String dbid, long id1, long link_type,
                                long[] id2s) throws Exception {

    BasicDBObject linkFind=new BasicDBObject();
    linkFind.put("id1", id1);
    linkFind.put("link_type", link_type);
    BasicDBList id2List=new BasicDBList();
    for (long id2 : id2s) {
      id2List.add(new Long(id2));
    }
    linkFind.put("id2", new BasicDBObject(
        "$in", id2List
    ));
    DBCursor linkResult=linkColl.find(linkFind);
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("link find:" + linkFind);
    }
    
    int count = linkResult.count();
    Link results[] = new Link[count];
    int i=0;
    while (linkResult.hasNext()) {
      Link l = createLinkFromRow(linkResult.next());
      if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
        logger.trace("Lookup result: " + id1 + "," + link_type + "," +
                  l.id2 + " found");
      }
      results[i++] = l;
    }
    return results;
  }

  // lookup using just id1, type
  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
    throws Exception {
    // Retry logic in getLinkList
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
    throws Exception {
    while (true) {
      try {
        return getLinkListImpl(dbid, id1, link_type, minTimestamp,
                               maxTimestamp, offset, limit);
      } catch (MongoClientException  ex) {
        if (!processMongoDBException(ex, "getLinkListImpl")) {
          throw ex;
        }
      }
    }
  }

  private Link[] getLinkListImpl(String dbid, long id1, long link_type,
        long minTimestamp, long maxTimestamp,
        int offset, int limit)
            throws Exception {
    
    BasicDBObject linkFind = new BasicDBObject();
    linkFind.put("id1", id1);
    linkFind.put("link_type", link_type);
    linkFind.put("visibility", LinkStore.VISIBILITY_DEFAULT);
    linkFind.put("time", new BasicDBObject("$gte", minTimestamp));
    linkFind.put("time", new BasicDBObject("$lte", maxTimestamp));
    
    DBCursor linkResult=linkColl.find(linkFind).
      sort(new BasicDBObject("time",-1)).
      skip(offset).
      limit(limit);
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("link find:" + linkFind);
    }

    int size = linkResult.size();

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Range lookup result: " + id1 + "," + link_type +
                         " is " + size);
    }
    if (size == 0) {
      return null;
    }

    // Fetch the link data
    Link links[] = new Link[size];
    int i = 0;
    while (linkResult.hasNext()) {
      Link l = createLinkFromRow(linkResult.next());
      links[i] = l;
      i++;
    }
    assert(i == size);
    return links;
  }

  private Link createLinkFromRow(DBObject dbLink) {
    Link l = new Link();
    l.id1 = ((Long)dbLink.get("id1")).longValue();
    l.id2 = ((Long)dbLink.get("id2")).longValue();
    l.link_type = ((Long)dbLink.get("link_type")).longValue();
    l.visibility = ((Integer)dbLink.get("visibility")).byteValue();
    l.data = (byte[])dbLink.get("data");
    l.time = ((Long)dbLink.get("time")).longValue();
    l.version = ((Integer)dbLink.get("version")).intValue();
    return l;
  }

  // count the #links
  @Override
  public long countLinks(String dbid, long id1, long link_type)
    throws Exception {
    while (true) {
      try {
        return countLinksImpl(dbid, id1, link_type);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "countLinks")) {
          throw ex;
        }
      }
    }
  }

  private long countLinksImpl(String dbid, long id1, long link_type)
        throws Exception {
    long count = 0;
    
    BasicDBObject countFind=new BasicDBObject();
    countFind.put("id",id1);
    countFind.put("link_type",link_type);

    DBCursor countResult = countColl.find(countFind);
    
    boolean found = false;

    if (countResult.hasNext()) {
      // found
      if (found) {
        logger.trace("Count query 2nd row!: " + id1 + "," + link_type);
      }

      found = true;
      count = ((Long)countResult.next().get("count")).longValue();
    }

    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("Count result: " + id1 + "," + link_type +
                         " is " + found + " and " + count);
    }

    return count;
  }

  @Override
  public int bulkLoadBatchSize() {
    return bulkInsertSize;
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
      throws Exception {
    while (true) {
      try {
        addBulkLinksImpl(dbid, links, noinverse);
        return;
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "addBulkLinks")) {
          throw ex;
        }
      }
    }
  }

  private void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse)
      throws Exception {
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkLinks: " + links.size() + " links");
    }

    addLinksNoCount(links);
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> counts)
                                                throws Exception {
    while (true) {
      try {
        addBulkCountsImpl(dbid, counts);
        return;
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "addBulkCounts")) {
          throw ex;
        }
      }
    }
  }

  private void addBulkCountsImpl(String dbid, List<LinkCount> counts)
                                                throws Exception {
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkCounts: " + counts.size() + " link counts");
    }
    if (counts.size() == 0)
      return;

    BulkWriteOperation bulkCounts = countColl.initializeUnorderedBulkOperation();
    
    for (LinkCount count : counts) {
      BasicDBObject countKey=new BasicDBObject();
      countKey.put("id", count.id1);
      countKey.put("link_type", count.link_type);
      
      BulkWriteRequestBuilder  bulkWriteRequestBuilder=bulkCounts.find(countKey);

      BasicDBObject countObj=new BasicDBObject();
      countObj.put("id", count.id1);
      countObj.put("link_type", count.link_type);
      countObj.put("count", count.count);
      countObj.put("time", count.time);
      countObj.put("version", count.version);
      
      BulkUpdateRequestBuilder updateCount = bulkWriteRequestBuilder.upsert();

      updateCount.replaceOne(countObj);
      
    }
    
    BulkWriteResult result=bulkCounts.execute();
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("bulk counts:"+result);
    }

  }

  private void checkNodeTableConfigured() throws Exception {
    if (this.nodetable == null) {
      throw new Exception("Nodetable not specified: cannot perform node" +
          " operation");
    }
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    checkNodeTableConfigured();
    
    nodeColl.remove(new BasicDBObject());
  }

  @Override
  public long addNode(String dbid, Node node) throws Exception {
    while (true) {
      try {
        return addNodeImpl(dbid, node);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "addNode")) {
          throw ex;
        }
      }
    }
  }

  private long addNodeImpl(String dbid, Node node) throws Exception {
    long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
    assert(ids.length == 1);
    return ids[0];
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    while (true) {
      try {
        return bulkAddNodesImpl(dbid, nodes);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "bulkAddNodes")) {
          throw ex;
        }
      }
    }
  }

  private long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws Exception {
    checkNodeTableConfigured();

    BulkWriteOperation bulkWriteOperation = nodeColl.
        initializeUnorderedBulkOperation();

    int i=0;
    long newIds[] = new long[nodes.size()];
    for (Node node : nodes) {
      BasicDBObject nodeObj=new BasicDBObject();
      long thisId=NodeAutoIncrement.getInstance().getNextSequence();
      nodeObj.put("id", thisId);
      nodeObj.put("type",node.type);
      nodeObj.put("version",node.version);
      nodeObj.put("time",node.time);
      nodeObj.put("data",node.data);
      bulkWriteOperation.insert(nodeObj);
      newIds[i++] = thisId;
    }
    BulkWriteResult nodeResult = bulkWriteOperation.execute();
    
    int objs=nodeResult.getInsertedCount();
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("bulk insert nodes: "+bulkWriteOperation);
    }

    if (i != nodes.size()) {
      throw new Exception("Wrong number of generated keys on insert: "
          + " expected " + nodes.size() + " actual " + i);
    }

    if (nodes.size() != objs) {
      throw new Exception("Wrong number of inserted objects: "
          + " expected " + nodes.size() + " actual " + i);
    }
    
    return newIds;
  }

  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    while (true) {
      try {
        return getNodeImpl(dbid, type, id);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "getNode")) {
          throw ex;
        }
      }
    }
  }

  private Node getNodeImpl(String dbid, int type, long id) throws Exception {
    checkNodeTableConfigured();
    
    BasicDBObject nodeKey = new BasicDBObject();
    nodeKey.put("id", id);
    
    DBCursor nodeCurr = nodeColl.find(nodeKey);
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("get node: "+nodeKey);
    }

    Node res=null;
    if (nodeCurr.hasNext()) {
      DBObject nodeObj=nodeCurr.next();
      res = new Node(
          ((Long)nodeObj.get("id")).longValue(), 
          ((Integer)nodeObj.get("type")).intValue(), 
          ((Long)nodeObj.get("version")).longValue(), 
          ((Integer)nodeObj.get("time")).intValue(), 
          (byte[])nodeObj.get("data"));
    }
    
    assert(nodeCurr.hasNext() == false);

    if (res ==null || res.type != type)
      return null;
    return res;
  }

  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {
    while (true) {
      try {
        return updateNodeImpl(dbid, node);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "updateNode")) {
          throw ex;
        }
      }
    }
  }

  private boolean updateNodeImpl(String dbid, Node node) throws Exception {
    checkNodeTableConfigured();
    
    BasicDBObject nodeKey = new BasicDBObject();
    nodeKey.put("id", node.id);
    nodeKey.put("type", node.type);
    
    BasicDBObject nodeObj=new BasicDBObject();
    nodeObj.put("version",node.version);
    nodeObj.put("time",node.time);
    nodeObj.put("data",node.data);
    
    WriteResult nodeRes = nodeColl.update(nodeKey, 
        new BasicDBObject("$set",nodeObj)
    );
    
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("node update:"+nodeKey+","+nodeObj);
    }

    int objs = nodeRes.getN();

    if (objs == 1) return true;
    else if (objs == 0) return false;
    else throw new Exception("Did not expect " + objs +  "affected objects: only "
        + "expected update to affect at most one object");
  }

  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    while (true) {
      try {
        return deleteNodeImpl(dbid, type, id);
      } catch (MongoClientException ex) {
        if (!processMongoDBException(ex, "deleteNode")) {
          throw ex;
        }
      }
    }
  }

  private boolean deleteNodeImpl(String dbid, int type, long id) throws Exception {
    checkNodeTableConfigured();
    
    BasicDBObject nodeKey = new BasicDBObject();
    nodeKey.put("id", id);
    nodeKey.put("type", type);
    
    WriteResult nodeRes = nodeColl.remove(nodeKey);

    int objs = nodeRes.getN();
    
    if (objs == 0) {
      return false;
    } else if (objs == 1) {
      return true;
    } else {
      throw new Exception(objs + " objects modified on delete: should delete " +
                      "at most one object");
    }
  }

}
