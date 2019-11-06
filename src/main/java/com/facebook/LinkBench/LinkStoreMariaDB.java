/*
 * Copyright 2019, Christine F. Reilly, Skidmore College
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
 *
 * This class implements LinkBench on the MariaDB version of the GraphMore
 * graph storage system. This class is modeled after the LinkStoreMysql class
 * from LinkBench.
 *
 * The GraphMore package for MariaDB is required for the functionality of this
 * class. The GraphMore package will be publically available as free and open
 * souce software following the publication of research that describes GraphMore.
 */
package com.facebook.LinkBench;

import edu.skidmore.graphmore.mariadb.*;
import edu.skidmore.graphmore.model.*;
import java.nio.*; // for ByteBuffer
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class LinkStoreMariaDB extends GraphStore {

  /** Label in the Properties object for the database information file */
  public static final String CONFIG_DBINFO = "dbinfo";

  /** Label in the Properties object for bulk insert batch size */
  public static final String CONFIG_BULK_INSERT_BATCH = "mariadb_bulk_insert_batch";


  /** Default number of items for bulk inserts */
  public static final int DEFAULT_BULKINSERT_SIZE = 1024;

  /** Object that represents the graph storage system */
  private GraphMoreMariaDB graphDB;

  /** Number of items for bulk inserts */
  private int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

  /**
  * Default constructor
  */
  public LinkStoreMariaDB() {
    super();
  }

  /**
  * Constructor initializes the GraphMore object.
  */
  public LinkStoreMariaDB(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  /**
  * Initialize the node store, this is inherited from NodeStore.
  * This implementation does not use currentPhase or threadId.
  *
  * @param props Contains the configuration information.
  * @param currentPhase Not used by this implementation.
  * @param threadId Not used by this implementation.
  */
  public void initialize(Properties props, Phase currentPhase, int threadId) throws IOException, Exception {
    // Extract database connection file pathname from the Properties object.
    String infoFile = ConfigUtil.getPropertyRequired(props, CONFIG_DBINFO);

    // Create the GraphMoreMariaDB object
    graphDB = new GraphMoreMariaDB(infoFile);

    // Set bulk insert batch size based on configuration file;
    // If this property is included in the configuration file.
    if (props.containsKey(CONFIG_BULK_INSERT_BATCH)) {
      bulkInsertSize = ConfigUtil.getInt(props, CONFIG_BULK_INSERT_BATCH);
    }
  }

  /**
  * Release resources when done using the graph store.
  */
  @Override
  public void close() {
    // Close the graphDB object at the end of using it
    graphDB.close();
  }

  /**
   * Add provided link to the store.  If already exists, update with new data
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * @param dbid
   * @param a
   * @param noinverse
   * @return true if new link added, false if updated. Implementation is
   *              optional, for informational purposes only.
   * @throws Exception
   */
  @Override
  public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
    try {
      // Create a ByteBuffer for the data payload
      ByteBuffer payloadBuf = ByteBuffer.allocate(a.data.length);
      payloadBuf.put(a.data);

      // Add the edge to the graphDB
      // GraphMoreMariaDB sets visibility to the code for visible on an add
      graphDB.edgeAdd(a.id1, a.id2, (int)a.link_type, a.version, payloadBuf);

      // edge add was successful if exception is not thrown
      return true;
    } catch (GraphMoreNodeNotFoundException e) {
      System.out.println("LinkStoreMariaDB: addLink got exception");
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Delete link identified by parameters from store.
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param id2
   * @param noinverse
   * @param expunge if true, delete permanently.  If false, hide instead
   * @return true if row existed. Implementation is optional, for informational
   *         purposes only.
   * @throws Exception
   */
  @Override
  public boolean deleteLink(String dbid, long id1, long link_type,
         long id2, boolean noinverse, boolean expunge) throws Exception {
    // LinkStoreMysql has two types of delete: set visibility to not visible,
    // or delete the link from the MySQL database.
    // This MariaDB GraphStore implementation will simply call the GraphStore
    // delete edge method.
    try {
      graphDB.edgeDelete(id1, id1, (int)link_type);
      return true;
    } catch (GraphMoreEdgeNotFoundException e) {
      return false;
    }
  }

  /**
   * Update a link in the database, or add if not found
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * @param dbid
   * @param a
   * @param noinverse
   * @return true if link found, false if new link created.  Implementation is
   *      optional, for informational purposes only.
   * @throws Exception
   */
  @Override
  public boolean updateLink(String dbid, Link a, boolean noinverse)
    throws Exception {
      // The logic to update database is in addLink
      boolean added = addLink(dbid, a, noinverse);

      // return value is opposite of addLink
      return !added;
    }

  /**
   *  lookup using id1, type, id2
   *  Returns hidden links.
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param id2
   * @return
   * @throws Exception
   */
  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
    throws Exception {
    // Modeled after LinkStoreMysql, call multigetLinks with id2 array of size one.
    // return the first Link array element.
    // Not implementing the size checks that LinkStoreMysql has.
    Link[] res = multigetLinks(dbid, id1, link_type, new long[] {id2});
    return res[0];
  }

  /**
   * Lookup multiple links: same as getlink but retrieve
   * multiple ids
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * @return list of matching links found, in any order
   */
  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
                                long id2s[]) throws Exception {

    ArrayList<GraphMoreEdge> edgeList = graphDB.getEdges(id1, id2s, (int)link_type);
    return graphMoreEdgeToLinks(edgeList);
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * LinkStoreMysql returns in descending order of time; this method does
   * not guarantee an order.
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @return list of links, or null if no matching links
   * @throws Exception
   */
  public Link[] getLinkList(String dbid, long id1, long link_type)
    throws Exception {

    //TODO test this method.

    ArrayList<GraphMoreEdge> edgeList = graphDB.getEdges(id1, (int)link_type);
    return graphMoreEdgeToLinks(edgeList);

  }

  /**
  * Copy GraphMore Edge list to a LinkBench Link array
  *
  * @param edgeList List of GraphMore edges.
  *
  * @return array of LinkBench links.
  */
  private Link[] graphMoreEdgeToLinks(ArrayList<GraphMoreEdge> edgeList) {

    Link[] links = new Link[edgeList.size()];

    for(int i = 0; i < edgeList.size(); i++) {

      // Get the GraphMoreEdge object
      GraphMoreEdge e = edgeList.get(i);

      // Copy GraphMore data payload into a byte array
      e.getData().flip(); // put ByteBuffer into read mode
      byte[] dataBytes = new byte[e.getData().limit()];
      e.getData().get(dataBytes); // copy ByteBuffer into dataBytes array

      // Add a link object to the links array
      // GraphMore edge does not have version, set version to 1
      links[i] = new Link(e.getOriginNode(), e.getEtype(), e.getDestinationNode(),
                  (byte)(e.getVisibility()), dataBytes, 1, e.getKey());
    }

    return links;
  }

  /**
   * lookup using just id1, type
   * Does not return hidden links
   *
   * Overrides method from LinkStore. Modeled after LinkStoreMysql.
   *
   * @param dbid
   * @param id1
   * @param link_type
   * @param minTimestamp
   * @param maxTimestamp
   * @param offset
   * @param limit
   * @return list of links in order of time, or null if no matching links
   * @throws Exception
   */
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
    throws Exception {

    //TODO implement this method. Overriding method from LinkStore, model after LinkStoreMysql
    // Requires new method added to GraphMoreEdgeSearch.
    return new Link[2];

  }

  //////// LinkStoreMysql has private method to create a Link object from a DB row in a result set

  /**
  * count the #links
  *
  * Overrides method from LinkStore. Modeled after LinkStoreMysql.
  *
  */
  @Override
  public long countLinks(String dbid, long id1, long link_type) throws Exception {

    //TODO implement this method.
    // requires new method added to GraphMoreEdgeSearch that returns the count of the edges

    return 0;
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
      throws Exception {

    //TODO test this method

    // Copy the list of Link objects to an ArrayList of GraphMoreEdge objects
    ArrayList<GraphMoreEdge> edges = new ArrayList<GraphMoreEdge>(links.size());
    for(int i = 0; i < links.size(); i++) {
      Link e = links.get(i);

      // Copy the link data payload into a byte buffer
      ByteBuffer payloadBuf = ByteBuffer.allocate(e.data.length);
      payloadBuf.put(e.data);

      // Create a GraphMoreEdge object, and add it to the ArrayList
      GraphMoreEdge gme = new GraphMoreEdge(-1, e.id1, e.id2, (int)e.link_type,
                              e.visibility, e.time, payloadBuf);
      edges.add(gme);
    }

    graphDB.edgeBulkAdd(edges);

    //TODO delete this initial implementation code after testing the better implementation (commented out for now)
    // Initial implementation: call addLink for each node in the list
    /*
    for(int i = 0; i < links.size(); i++) {
      addLink(dbid, links.get(i), noinverse);
    }
    */
  }

  /**
  * Overrides method from LinkStore.
  * Not acutally implemented for LinkStoreMariaDB, method simply returns.
  * Need to override the LinkStore method so that the UnsupportedOperationException
  * is not thrown when the load program runs. If the workload program needs the
  * count table, then this method will need to be implemented and a count
  * table will need to be added to the DB.
  */
  @Override
  public void addBulkCounts(String dbid, List<LinkCount> counts)
                                                throws Exception {
    return;
  }


  /**
   * Reset node storage to a clean state in shard:
   *   deletes all stored nodes
   *   resets id allocation, with new IDs to be allocated starting from startID
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql.
  *
  */
  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    //TODO implement this method.
    return;
  }

  /**
   * Adds a new node object to the database.
   *
   * This allocates a new id for the object and returns i.
   *
   * The benchmark assumes that, after resetStore() is called,
   * node IDs are allocated in sequence, i.e. startID, startID + 1, ...
   * Add node should return the next ID in the sequence.
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql.
   *
   * @param dbid the db shard to put that object in
   * @param node a node with all data aside from id filled in.  The id
   *    field is *not* updated to the new value by this function
   * @return the id allocated for the node
   */
  @Override
  public long addNode(String dbid, Node node) throws Exception {
    // Create a ByteBuffer for the data payload
    ByteBuffer dataBuffer = ByteBuffer.allocate(node.data.length);
    dataBuffer.put(node.data);

    // Add the node to the graphDB
    long id = graphDB.nodeAdd(node.type, dataBuffer);

    return id;
  }

  /**
   * Bulk loading to more efficiently load nodes.
   * Calling this is equivalent to calling addNode multiple times.
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql. Node fields
  * to be added are type, version, and data. Let id and updateTime be set
  * automatically by the database.
  *
   * @param dbid
   * @param nodes
   * @return the actual IDs allocated to the nodes
   * @throws Exception
   */
  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {

    //TODO test this method

    // Copy the list of Node objects to an ArrayList of GraphMoreNode objects
    ArrayList<GraphMoreNode> gmNodes = new ArrayList<GraphMoreNode>(nodes.size());
    for(int i = 0; i < nodes.size(); i++) {
      Node n = nodes.get(i);

      // Copy the node data payload into a byte buffer
      ByteBuffer payloadBuf = ByteBuffer.allocate(n.data.length);
      payloadBuf.put(n.data);

      GraphMoreNode gmn = new GraphMoreNode(-1, n.type, n.version, 0, payloadBuf);
      gmNodes.add(gmn);
    }

    // Boolean parameter to nodeBulkAdd indicates that system should generate
    // node ids
    long[] ids = graphDB.nodeBulkAdd(gmNodes, true);

    //TODO delete this initial implementation code after testing the better implementation (commented out for now)
    // Initial implementation: call addNode for each node in the list
    /*
    for(int i = 0; i < nodes.size(); i++) {
      ids[i] = addNode(dbid, nodes.get(i));
    }
    */

    return ids;
  }

  /**
   * Preferred size of data to load
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql.
  *
   * @return
   */
  @Override
  public int bulkLoadBatchSize() {
    return bulkInsertSize;
  }

  /**
   * Get a node of the specified type
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql.
  *
   * @param dbid the db shard the id is mapped to
   * @param type the type of the object
   * @param id the id of the object
   * @return null if not found, a Node with all fields filled in otherwise
   */
  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    try {
      GraphMoreNode gmNode = graphDB.nodeGet(id);

      // If the type does not match, return null (following LinkStoreMysql)
      if (gmNode.getNtype() != type) {
        return null;
      }

      // Translate the GraphMoreNode into a LinkBench Node

      // GraphMoreNode stores the last updated time as a Java Date object.
      // Java's getTime method returns the Date object as a long UNIX timestamp.
      // LinkBench Node uses int for time.
      // Using int for UNIX timestamp raises the year 2038 bug!
      // Going to let timestamp be int to conform to LinkBench code.
      int timeInt = (int)(gmNode.getUpdateTime().getTime());

      // Flip the buffer to make sure it is in read mode
      gmNode.getDataPayload().flip();

      // Create a byte[] to hold the buffer contents
      byte[] dataBytes = new byte[gmNode.getDataPayload().limit()];

      // Copy buffer contents into byte array
      gmNode.getDataPayload().get(dataBytes);

      return new Node(gmNode.getId(), gmNode.getNtype(), gmNode.getVersion(), timeInt, dataBytes);

    } catch (GraphMoreNodeNotFoundException e) {
      // Following what is done in LinkStoreMysql
      // If no node is found, this method returns null
      return null;
    }
  }

  /**
   * Update all parameters of the node specified.
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql.
  *
   * @param dbid
   * @param node
   * @return true if the update was successful, false if not present
   */
  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {

    // GraphMore's nodeUpdate method only specifies the payload for the node.
    // The version and update time are automatically set.
    // LinkStoreMysql uses the version and time from the node parameter to
    // set these.
    // Another difference is LinkStoreMysql looks up the node by id and type.
    // GraphMore only uses the id.
    // I will try using the current version of GraphMore nodeUpdate.
    // If LinkBench has failues I may need to add another version of nodeUpdate
    // to GraphMore that takes the version and time as parameters, and also
    // uses the type to find the node.

    try {
      // Put the node payload into a ByteBuffer
      ByteBuffer payloadBuf = ByteBuffer.allocate(node.data.length);
      payloadBuf.put(node.data);

      // Call GraphMore's nodeUpdate method
      graphDB.nodeUpdate(node.id, payloadBuf);

      // Assume the update is successful if no exception was thrown
      return true;

    } catch (GraphMoreNodeNotFoundException e) {
      return false;
    }
  }

  /**
   * Delete the object specified by the arguments
  *
  * Overrides method from NodeStore. Modeled after LinkStoreMysql.
  *
   * @param dbid
   * @param type
   * @param id
   * @return true if the node was deleted, false if not present
   */
  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    // LinkStoreMysql looks up the node by id and type. GraphMore only uses the id.
    // I will try using the current version of GraphMore nodeDelete.
    // If LinkBench has failues I may need to add another version of nodeDelete
    // to GraphMore that also uses the type to find the node.

    try {
      graphDB.nodeDelete(id);
      return true;

    } catch (GraphMoreNodeNotFoundException e) {
      return false;
    }
  }

  public void clearErrors(int loaderId) {
    // in LinkStoreMysql, reopens the database connection
    //TODO implement this method.
    return;
  }

}
