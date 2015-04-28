package com.facebook.LinkBench.store.mongodb;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.LinkStore;

import java.util.ArrayList;

public class MongoPrototype {

    public static void main(String[] args) {
        LinkStoreMongoDB graphStore = new LinkStoreMongoDB();
        try {
            
            System.out.println("Mongo Graph");
            
            graphStore.initialize(null, null, 0);

            graphStore.resetNodeStore(null, 0);

            // Add nodes
            Node node1 = new Node(1, 100, 9, 9, new byte[0]);
            Node node2 = new Node(2, 200, 9, 9, new byte[0]);
            graphStore.addNode(null, node1);
            graphStore.addNode(null, node2);

            // Add link
            Link link = new Link(node1.id, 999, node2.id, new Byte("0"), new byte[0], 9, 9);
            graphStore.addLink(null, link, false);

            // Get link
            graphStore.getLink(null, node1.id, 999, node2.id);

            // Get nodes
            graphStore.getNode(null, 100, node1.id);
            graphStore.getNode(null, 100, node2.id);

            // Count vertex edges
            graphStore.countLinks(null, node1.id, 999);

            // Update vertex
            Node node3 = new Node(3, 100, 9, 9, new byte[0]);
            graphStore.addNode(null, node3);
            graphStore.updateNode(null, node3);

            // MultiGetlinks
            Node node4 = new Node(4, 100, 9, 9, new byte[0]);
            Node node5 = new Node(5, 200, 9, 9, new byte[0]);
            graphStore.addNode(null, node4);
            graphStore.addNode(null, node5);
            graphStore.multigetLinks(null, node2.id, 999, new long[] {node4.id, node5.id});
            graphStore.multigetLinks(null, node1.id, 999, new long[] {node2.id, node5.id});

            // Delete link
            Link link2 = new Link(node1.id, 9999, node2.id, LinkStore.VISIBILITY_DEFAULT, new byte[0], 9, 9);
            graphStore.addLink(null, link2, false);
            graphStore.deleteLink(null, node1.id, link2.link_type, node2.id, false, false);

            // Delete link with expunge
            Link link3 = new Link(node1.id, 99999, node2.id, LinkStore.VISIBILITY_DEFAULT, new byte[0], 9, 9);
            graphStore.addLink(null, link3, false);
            graphStore.deleteLink(null, node1.id, link3.link_type, node2.id, false, true);

            // Add bulk links
            Link link4 = new Link(node1.id, 99993, node2.id, LinkStore.VISIBILITY_DEFAULT, new byte[0], 9, 9);
            Link link5 = new Link(node1.id, 99994, node2.id, LinkStore.VISIBILITY_DEFAULT, new byte[0], 9, 9);
            ArrayList<Link> links = new ArrayList<Link>(2);
            links.add(link4);
            links.add(link5);
            graphStore.addBulkLinks(null, links, false);

            // Get link list
            Node node7 = new Node(7, 100, 9, 9, new byte[0]);
            graphStore.addNode(null, node7);
            Node node8 = new Node(8, 100, 9, 9, new byte[0]);
            graphStore.addNode(null, node8);
            Node node9 = new Node(9, 100, 9, 9, new byte[0]);
            graphStore.addNode(null, node9);

            Link link6 = new Link(node7.id, 9990, node8.id, LinkStore.VISIBILITY_DEFAULT, new byte[0], 9, 9);
            Link link7 = new Link(node7.id, 9990, node9.id, LinkStore.VISIBILITY_DEFAULT, new byte[0], 9, 9);
            graphStore.addLink(null, link6, false);
            graphStore.addLink(null, link7, false);
            graphStore.getLinkList(null, node7.id, 9990, 1, 10, 0, 100);

            // Delete Node
            graphStore.deleteNode(null, node1.type, node1.id);
            graphStore.deleteNode(null, node2.type, node2.id);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}