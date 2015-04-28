package com.facebook.LinkBench.store.mongodb;

import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.LinkStoreFactory;

/**
 * {@link com.facebook.LinkBench.store.LinkStoreFactory} for MySQL.
 */
public class MongoDBLinkStoreFactory implements LinkStoreFactory {

    @Override
    public LinkStore createLinkStore() {
        return new LinkStoreMongoDB();
    }
}
