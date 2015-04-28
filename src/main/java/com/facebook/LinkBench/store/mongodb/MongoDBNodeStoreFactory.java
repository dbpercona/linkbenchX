package com.facebook.LinkBench.store.mongodb;

import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.NodeStore;
import com.facebook.LinkBench.NodeStoreFactory;

/**
 * {@link com.facebook.LinkBench.NodeStoreFactory} for MySQL.
 */
public class MongoDBNodeStoreFactory implements NodeStoreFactory {

    @Override
    public NodeStore createNodeStore(LinkStore linkStore) {
        if (!(linkStore instanceof NodeStore)) {
            return (NodeStore)new MongoDBLinkStoreFactory().createLinkStore();
        }

        return (NodeStore) linkStore;
    }
}
