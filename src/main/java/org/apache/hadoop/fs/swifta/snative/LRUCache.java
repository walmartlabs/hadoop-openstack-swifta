package org.apache.hadoop.fs.swifta.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LRU Cache for reduce HEAD requests.
 *
 */
public class LRUCache<T> {

  private final static Log LOG = LogFactory.getLog(LRUCache.class);
  private final Map<String, DLinkedNode> cache = new ConcurrentHashMap<String, DLinkedNode>();
  private int capacity;
  private DLinkedNode head, tail;
  private long liveTime;

  class DLinkedNode {
    String key;
    CacheObject<T> value;
    DLinkedNode pre;
    DLinkedNode post;
  }

  /**
   * Always add the new node right after head;
   */
  private void addNode(DLinkedNode node) {
    node.pre = head;
    node.post = head.post;

    head.post.pre = node;
    head.post = node;
  }

  /**
   * Remove an existing node from the linked list. The null pointer check can be removed in future.
   */
  private void removeNode(DLinkedNode node, boolean isRemove) {
    /**
     * Sometimes happen while running some of Presto queries.
     */
    if (node == null) {
      return;
    }
    String key = node.key;
    DLinkedNode pre = node.pre;
    DLinkedNode post = node.post;
    if (pre != null) {
      pre.post = post;
    }
    if (post != null) {
      post.pre = pre;
    }
    if (isRemove && key != null) {
      cache.remove(key);
    }
  }

  /**
   * Move certain node in between to the head.
   */
  private void moveToHead(DLinkedNode node) {
    this.removeNode(node, Boolean.FALSE);
    this.addNode(node);
  }

  private void init(int capacity) {
    this.capacity = capacity;

    head = new DLinkedNode();
    head.pre = null;

    tail = new DLinkedNode();
    tail.post = null;

    head.post = tail;
    tail.pre = head;
  }

  public LRUCache(int capacity, long liveTime) {
    this.init(capacity);
    this.liveTime = liveTime;
  }

  public T get(String key) {

    DLinkedNode node = cache.get(key);
    if (node == null) {
      return null;
    }

    /**
     * Only expires cache when use.
     */
    if (node.value == null || node.value.isExpired(liveTime)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Expiring cache entry " + key);
      }
      this.removeNode(node, Boolean.TRUE);
      return null;
    }
    // Move the accessed node to the head.
    this.moveToHead(node);
    node.value.setAccessTime(System.currentTimeMillis());
    return node.value.getValue();
  }

  public boolean remove(String key) {

    DLinkedNode node = cache.get(key);
    if (node == null) {
      return Boolean.FALSE;
    }
    this.removeNode(node, Boolean.TRUE);
    return Boolean.TRUE;
  }

  public int getSize() {
    return cache.size();
  }

  public void set(String key, T value) {
    DLinkedNode node = cache.get(key);

    if (node == null) {

      DLinkedNode newNode = new DLinkedNode();
      newNode.key = key;
      newNode.value = new CacheObject<T>(value);

      this.cache.put(key, newNode);
      this.addNode(newNode);

      if (cache.size() > capacity) {
        this.removeNode(tail.pre, Boolean.TRUE);
      }
    } else {
      // update the value.
      node.value.setValue(value);
      node.value.setAccessTime(System.currentTimeMillis());
      this.moveToHead(node);
    }
  }
}
