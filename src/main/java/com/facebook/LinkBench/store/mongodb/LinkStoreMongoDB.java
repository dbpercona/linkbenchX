package com.facebook.LinkBench.store.mongodb;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import com.facebook.LinkBench.GraphStore;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A rough implementation for using LinkBench with Blueprints-enabled graphs.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LinkStoreMongoDB extends GraphStore {

    MongoDBGraph graph;
    static final AtomicLong maxLoadId = new AtomicLong(System.currentTimeMillis());

    @Override
    public void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, Exception {
        this.graph = new MongoDBGraph();
    }

    @Override
    public void close() {
        graph.close();
    }

    @Override
    public void clearErrors(int threadID) {
        // ??
    }

    @Override
    public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
        return  graph.addLink(a);
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        try {
            Iterator<DBObject> cursor = graph.getLink(id1, id2, link_type);
            while (cursor.hasNext()) {
                BasicDBObject edge = (BasicDBObject) cursor.next();
                if (expunge) {
                    graph.removeLink(edge);
                } else {
                    edge.append(MongoDBGraph.PROPERTY_VISIBILITY, (int) VISIBILITY_HIDDEN);
                }

            }
            return true;
        } catch (Exception ex) {
            
            return false;
        }
    }

    @Override
    public boolean updateLink(String dbid, Link link, boolean noinverse) throws Exception {
        try {
            Iterator<DBObject> cursor = graph.getLink(link.id1, link.id2, link.link_type);

            while (cursor.hasNext()) {
                BasicDBObject edge = (BasicDBObject) cursor.next();
                graph.updateEdgeProperties(edge, link);
            }
            return true;
        } catch (Exception ex) {
            
            return false;
        }
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        Iterator<DBObject> cursor = graph.getLink(id1, id2, link_type);
        if (cursor.hasNext()) {
            return createLink(id1, link_type, id2, (BasicDBObject) cursor.next());
        }
        return null;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        DBCursor cursor =  graph.findLinks(id1, link_type);
        if (cursor == null)
            System.out.println(id1);
        final List<Link> links  = new ArrayList<Link>();
        for (DBObject link : cursor) {
            Link l = createLink(id1,link_type, (BasicDBObject) link);
            if (l!=null)
                links.add(l);
        }
        return links.toArray(new Link[links.size()]);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        DBCursor cursor =  graph.findLinks(id1, minTimestamp, maxTimestamp, link_type);
        cursor.skip(offset);
        final List<Link> links  = new ArrayList<Link>();
        for (DBObject e : cursor) {
            final Link l = createLink(id1, link_type, (BasicDBObject)e);
            if (l != null && l.visibility == VISIBILITY_DEFAULT &&
                    l.time >= minTimestamp && l.time <= maxTimestamp) {
                links.add(l);
                if (links.size() >= limit) {
                    break;
                }
            }
        }
        return links.toArray(new Link[links.size()]);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        return graph.findLinks(id1,link_type).size();
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        graph.drop();

    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        if(node.id == -1){
            node.id = maxLoadId.getAndIncrement();
        }
        graph.addNode(node);
        return node.id;
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {

        DBRef ref = graph.getNodeRef(id);
        if (ref == null) {
            return null;
        }
        BasicDBObject v = (BasicDBObject) ref.fetch();
        return new Node(id,
                v.getInt(MongoDBGraph.PROPERTY_TYPE),
                v.getLong(MongoDBGraph.PROPERTY_VERSION),
                v.getInt(MongoDBGraph.PROPERTY_TIME),
                (byte[]) v.get(MongoDBGraph.PROPERTY_DATA));
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        try {
            DBRef ref = graph.getNodeRef(node.id);
            if (ref == null) {
                return false;
            }

            graph.updateVertexProperties((BasicDBObject) ref.fetch(), node);
            

            return true;
        } catch (Exception ex) {
            
            return false;
        }
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        // Blueprints doesn't care about the dbid or the type. The id is identifier enough to uniquely identify it.
        try {
            DBRef ref = graph.getNodeRef(id);
            if (ref == null) {
                return false;
            }
            graph.deleteNode(ref);
            return true;
        } catch (Exception ex) {
            
            return false;
        }
    }


    private static Link createLink(final long id1, final long link_type, final BasicDBObject e) {
        try {
            // defensively check for the vertex in case it got blown away in a delete.  need try-catch as some
            // graph implementations will throw an exception here if the vertex is no longer present
            DBRef ref2 = (DBRef)e.get("v2");
            BasicDBObject v2 = (BasicDBObject) ref2.fetch();
            if (v2 == null) {
                return null;
            }

            return createLink(id1, v2.getLong(MongoDBGraph.PROPERTY_IID), link_type, e);
        } catch (Exception ex) {
            return null;
        }
    }

    private static Link createLink(final long id1, final long id2, final long link_type, final BasicDBObject e) {
        return new Link(id1, link_type, id2,
                (byte) e.getInt(MongoDBGraph.PROPERTY_VISIBILITY),
                (byte[]) e.get(MongoDBGraph.PROPERTY_DATA),
                e.getInt(MongoDBGraph.PROPERTY_VERSION),
                e.getLong(MongoDBGraph.PROPERTY_TIME)
        );
    }


    Long inicio = 0l;
    @Override
    public void addBulkLinks(String dbid, final List<Link> links, boolean noinverse) {
        Long time = System.nanoTime();
        System.out.println("Entrou "+ inicio++);
        graph.addLinks(links);
        System.out.println("Fim depois de "+ (System.nanoTime()-time)/100000000.0+ "s");
    }

    public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
        return graph.addNodes(nodes);
    }

}