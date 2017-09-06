package org.apache.hadoop.fs.swifta.snative;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LFUCache<T> {

  private static final Log LOG = LogFactory.getLog(LFUCache.class);

  /**
   * Store node position in doubly linked list [key, Node].
   */
  private Map<String, LFUNode> positionNodes = new HashMap<String, LFUNode>();

  /**
   * Store access count and most recently used node among all of the same access count nodes
   * [access_count, most_recently_node].
   */
  private Map<Integer, LFUNode> countMap = new ConcurrentHashMap<Integer, LFUNode>();
  private int capacity;
  /**
   * Tail pointer, avoid null point checks.
   */
  private LFUNode tail;

  private long liveTime;

  /**
   * Create a new cache.
   *
   * @param capacity capacity
   * @param liveTime live time
   */
  public LFUCache(int capacity, long liveTime) {
    this.capacity = capacity;
    tail = new LFUNode(null, null);
    tail.count = -1;
    this.liveTime = liveTime;
  }

  public synchronized T get(String key) {
    LFUNode node = positionNodes.get(key);
    return (node == null) ? null : (this.expireCache(node, key) ? null : increaseCount(key));
  }

  /**
   * Expire the cache.
   * 
   * @param node node to check
   * @param key key
   * @return Has expired
   */
  private boolean expireCache(LFUNode node, String key) {

    if (node.val == null || node.val.isExpired(liveTime)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Expiring cache entry " + key);
      }
      this.removeNode(node);
      return Boolean.TRUE;
    }
    return Boolean.FALSE;

  }

  /**
   * Insert node post behind node pre.
   * 
   * @param pre previous node
   * @param post next node
   */
  private synchronized void insert(LFUNode pre, LFUNode post) {

    /**
     * Don't have to update the same node.
     */
    if (pre == post) {
      return;
    }
    LFUNode next = pre.next;
    pre.next = post;
    post.pre = pre;
    if (next != null) {
      next.pre = post;
    }
    post.next = next;
  }

  /**
   * Set value.
   *
   * @param key key
   * @param value value
   */
  public synchronized void set(String key, T value) {
    LFUNode n = positionNodes.get(key);
    LFUNode recentlyUsed = countMap.get(1);

    if (n == null) {
      /**
       * Always remove tail.
       */
      if (positionNodes.size() >= this.capacity) {
        this.removeNode(tail.next);
      }
      recentlyUsed = countMap.get(1);
      LFUNode newNode = new LFUNode(key, new CacheObject<T>(value));
      /**
       * New node will be the most recently used node among of all nodes with access time 1.
       */
      this.insert(((recentlyUsed == null) ? tail : recentlyUsed), newNode);
      countMap.put(1, newNode);
      positionNodes.put(key, newNode);
    } else {
      n.val = new CacheObject<T>(value);
      increaseCount(key);
    }
  }

  /**
   * Increase access time and adjust position in double linked list.
   * 
   * @param key key in map
   * @return value
   */
  private synchronized T increaseCount(String key) {
    LFUNode curNode = positionNodes.get(key);
    if (curNode == null) {
      return null;
    }
    LFUNode recentlyUsed = countMap.get(curNode.count + 1);
    if (recentlyUsed == null) {
      /**
       * If the count+1 number not in map, which means it can insert right after most recently used
       * node with the same count.
       */
      recentlyUsed = countMap.get(curNode.count);
    }

    curNode.val.setAccessTime(System.currentTimeMillis());

    if (recentlyUsed == null || recentlyUsed == curNode) {
      /**
       * No need to move position.
       */
      this.updateFrequence(curNode);
      curNode.count++;
      countMap.put(curNode.count, curNode);
    } else {
      int count = curNode.count;
      this.removeNode(curNode);
      LFUNode newNode = new LFUNode(key, curNode.val);
      newNode.count = count + 1;

      this.insert(recentlyUsed, newNode);
      countMap.put(count + 1, newNode);
      positionNodes.put(key, newNode);
    }
    return curNode.val.getValue();
  }

  /**
   * When removing a record from countMap, always try to find if other node with the same count can
   * promoted to most recently used node. Otherwise delete the record only.
   * 
   * @param node which node
   */
  private void updateFrequence(LFUNode node) {
    if (countMap.get(node.count) == node) {
      if (node.pre.count == node.count) {
        countMap.put(node.count, node.pre);
      } else {
        countMap.remove(node.count);
      }
    }
  }

  /**
   * Get size.
   *
   * @return size
   */
  public int getSize() {
    return positionNodes.size();
  }

  /**
   * Remove key.
   * 
   * @param key key
   * @return if removed
   */
  public synchronized boolean remove(String key) {
    LFUNode node = positionNodes.get(key);
    if (node == null) {
      return Boolean.FALSE;
    }
    this.removeNode(node);
    return Boolean.TRUE;
  }

  /**
   * Remove an existing node from the linked list.
   * 
   * @param node to delete.
   */
  private synchronized void removeNode(LFUNode node) {
    this.updateFrequence(node);
    LFUNode pre = node.pre;
    LFUNode post = node.next;
    pre.next = post;
    if (post != null) {
      post.pre = pre;
    }
    positionNodes.remove(node.key);

  }

  class LFUNode {
    String key;
    CacheObject<T> val;
    int count;
    LFUNode pre;
    LFUNode next;

    public LFUNode(String key, CacheObject<T> val) {
      this.key = key;
      this.val = val;
      this.count = 1;
    }

  }
}
