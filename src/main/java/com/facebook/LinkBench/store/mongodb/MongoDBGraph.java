package com.facebook.LinkBench.store.mongodb;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * User: felipe
 * Date: 2/27/14
 * Time: 9:07 PM
 */
public class MongoDBGraph {
    public static final String NODE_COLLECTION = "node";
    public static final String LINK_COLLECTION = "link";

    public static final String PROPERTY_DATA = "data";
    public static final String PROPERTY_IID = "_id";
    public static final String PROPERTY_TIME = "time";
    public static final String PROPERTY_TYPE = "type";
    public static final String PROPERTY_VERSION = "version";
    public static final String PROPERTY_VISIBILITY = "visibility";

    private MongoClient mongoClient;

    private DB g;
    private DBCollection nodeCollection;
    private DBCollection linkCollection;



    public MongoDBGraph() throws UnknownHostException {
        mongoClient = new MongoClient( "localhost" );
        // Executar ~/Workspace/mongodb$ bin/mongod -f mongodb.conf
        g = mongoClient.getDB("graph-linkbench");
        nodeCollection = g.getCollection(NODE_COLLECTION);
        linkCollection = g.getCollection(LINK_COLLECTION);
    }

    public void close() {
        mongoClient.close();
    }

    public DBRef  getNodeRef(Long id) {
        DBRef ref = new DBRef(g, "node", id);
        if (ref.fetch() == null)
            return null;
        return ref;
    }

    public DBObject createLinkDBObject(Link link) {
        DBRef reference1 = this.getNodeRef(link.id1);
        DBRef reference2 = this.getNodeRef(link.id2);
        if (reference1 == null || reference2 == null) {
            return null;
        }
        final BasicDBObject e = new BasicDBObject();
        e.append("v1", reference1);
        e.append("v2", reference2);
        updateEdgeProperties(e, link);
        return e;
    }

    public boolean addLinks(List<Link> links) {
        List<DBObject> listDbObject = new LinkedList<DBObject>();
        for (Link l : links) {
            DBObject obj = createLinkDBObject(l);
            if (obj!=null)
                listDbObject.add(obj);
        }
        linkCollection.insert(listDbObject);
        return true;
    }

    public boolean addLink(Link link) {
        List<Link> list = new LinkedList<Link>();
        list.add(link);
        return this.addLinks(list);
    }


    public Iterator<DBObject> getLink(long id1, long id2, long type) {
        DBRef reference1 = getNodeRef(id1);
        DBRef reference2 = getNodeRef(id2);

        if (reference1 == null || reference2 == null) {
            return new LinkedList<DBObject>().iterator();
        }

        BasicDBObject query = new BasicDBObject("v1",reference1).append("v2",reference2).append(PROPERTY_TYPE, type);
        return linkCollection.find(query);
    }


    public void updateEdgeProperties(final BasicDBObject e, final Link a) {
        e.append(PROPERTY_VISIBILITY, (int) a.visibility).
                append(PROPERTY_DATA, a.data).
                append(PROPERTY_TIME, a.time).
                append(PROPERTY_VERSION, a.version);
    }

    public void updateVertexProperties(final BasicDBObject v, final Node node) {
        v.append(PROPERTY_IID, node.id).
                append(PROPERTY_TYPE, node.type).
                append(PROPERTY_DATA, node.data).
                append(PROPERTY_TIME, node.time).
                append(PROPERTY_VERSION, node.version);
    }

    public void removeLink(BasicDBObject edge) {
        linkCollection.remove(edge);
    }


    public DBCursor findLinks(long id1, long link_type) {
        DBRef reference1 = this.getNodeRef(id1);
        if (reference1 == null) {
            return null;
        }
        BasicDBObject query = new BasicDBObject("v1",reference1).append(MongoDBGraph.PROPERTY_TYPE, link_type);
        return linkCollection.find(query);
    }

    public DBCursor findLinks(long id1, long minTimestamp, long maxTimestamp, long type) {
        DBRef reference1 = this.getNodeRef(id1);

        if (reference1 == null) {
            return null;
        }

        BasicDBObject query = new BasicDBObject("v1",reference1).append(PROPERTY_TYPE, type).
                append(PROPERTY_TIME, new BasicDBObject("$gt",minTimestamp)).
                append(PROPERTY_TIME, new BasicDBObject("$lt",maxTimestamp));

        return linkCollection.find(query).addSpecial( "$orderby", new BasicDBObject("age",-1) );
    }

    public void drop() {
        nodeCollection.drop();
        linkCollection.drop();
        nodeCollection = g.createCollection(NODE_COLLECTION, new BasicDBObject());
        linkCollection = g.createCollection(LINK_COLLECTION, new BasicDBObject());
    }

    public DBObject createNodeDBObject(Node node) {
        BasicDBObject v = new BasicDBObject();
        updateVertexProperties(v, node);
        return v;
    }

    public void deleteNode(DBRef ref) {
        nodeCollection.remove(ref.fetch());
    }

    public long[] addNodes(List<Node> nodes) {
        List<DBObject> listDbObject = new LinkedList<DBObject>();
        long[] ids = new long[nodes.size()];
        int i =0;
        for (Node n : nodes) {
            DBObject obj = createNodeDBObject(n);
            ids[i++] = n.id;
            if (obj!=null)
                listDbObject.add(obj);
        }
        nodeCollection.insert(listDbObject);
        return ids;
    }

    public void addNode(Node node) {
        List<Node> list = new LinkedList<Node>();
        list.add(node);
        this.addNodes(list);
    }
}
