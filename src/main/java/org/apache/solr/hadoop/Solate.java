/**
 * 
 */
package org.apache.solr.hadoop;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Solate {
	private static final Logger LOG = LoggerFactory.getLogger(Solate.class);
	private int shards;
	private HashMap<String, Integer> shardNumbers;
	private DocCollection docCollection;
	private final SolrParams emptySolrParams = new MapSolrParams(
			Collections.<String, String> emptyMap());

	public Solate(String zkHost, String collection, int numShards) {
		this.shards = numShards;
		if (shards <= 0) {
			throw new IllegalArgumentException("Illegal shards: " + shards);
		}
		if (zkHost == null) {
			throw new IllegalArgumentException("zkHost must not be null");
		}
		if (collection == null) {
			throw new IllegalArgumentException("collection must not be null");
		}
		LOG.info("Using SolrCloud zkHost: {}, collection: {}", zkHost,
				collection);
		docCollection = new ZooKeeperInspector()
				.extractDocCollection(zkHost,
				collection);
		if (docCollection == null) {
			throw new IllegalArgumentException("docCollection must not be null");
		}
		if (docCollection.getSlicesMap().size() != shards) {
			throw new IllegalArgumentException("Incompatible shards: + "
					+ shards + " for docCollection: " + docCollection);
		}
		LOG.info("Got slices: " + docCollection.getSlices().size());
		for (Slice s : docCollection.getSlices()) {
			LOG.info("Slice: " + s.getName());
		}
		List<Slice> slices = new ZooKeeperInspector()
				.getSortedSlices(docCollection.getSlices());
		if (slices.size() != shards) {
			throw new IllegalStateException("Incompatible sorted shards: + "
					+ shards + " for docCollection: " + docCollection);
		}
		shardNumbers = new HashMap<String, Integer>(slices.size());

		for (int i = 0; i < slices.size(); i++) {
			shardNumbers.put(slices.get(i).getName(), i);
		}
		LOG.debug("Using SolrCloud docCollection: {}", docCollection);
		DocRouter docRouter = docCollection.getRouter();
		if (docRouter == null) {
			throw new IllegalArgumentException("docRouter must not be null");
		}
		LOG.info("Using SolrCloud docRouterClass: {}", docRouter.getClass());
	}

	public int getPartition(String keyStr, SolrInputDocument doc) {
		DocRouter docRouter = docCollection.getRouter();

		Slice slice = docRouter.getTargetSlice(keyStr, doc, emptySolrParams,
				docCollection);

		if (slice == null) {
			throw new IllegalStateException(
					"No matching slice found! The slice seems unavailable. docRouterClass: "
							+ docRouter.getClass().getName());
		}

		int rootShard = shardNumbers.get(slice.getName());
		if (rootShard < 0 || rootShard >= shards) {
			throw new IllegalStateException("Illegal shard number " + rootShard
					+ " for slice: " + slice + ", docCollection: "
					+ docCollection);
		}

		LOG.debug("Slice " + slice.getName() + " == #" + rootShard);

	    return rootShard;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String zkHost = "openstack2.ad.bl.uk:2181,openstack4.ad.bl.uk:2181,openstack5.ad.bl.uk:2181/solr";
		String collection = "jisc2";
		int numShards = 4;

		Solate scp = new Solate(zkHost, collection, numShards);
		String key = "IDKey";
		SolrInputDocument value = new SolrInputDocument();
		scp.getPartition(key, value);

	}

}
