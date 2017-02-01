package org.apache.hadoop.fs.swifta.snative;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LRU Cache to reduce HEAD requests.
 *
 */
public class LRUCache<T> {


  private final static Log LOG = LogFactory.getLog(LRUCache.class);
  private final Map<String, DLinkedNode> cache = new ConcurrentHashMap<String, DLinkedNode>();
  private DLinkedNode head, tail;
  private int capacity;
  private long liveTime;

  class DLinkedNode {
	String key;
    CacheObject<T> value;
    DLinkedNode pre;
    DLinkedNode post;
  }

  // Insert the node before the current head and set the head to the node
  private void insertHeadNode(DLinkedNode node) {
	node.post = head;
	node.pre = null;
	if (head != null) {
	  head.pre = node; 
	}
	head = node;
	if (tail == null) {
	  tail = node;
	}
  }
  
  private void removeNode(DLinkedNode node) {	
    DLinkedNode pre = node.pre;
    DLinkedNode post = node.post;
    if (pre != null) {
      pre.post = post;
    } else {
      head = post;
    }
    if (post != null) {
      post.pre = pre;
    } else {
      tail = pre;
    }
  }

  public LRUCache(int capacity, long liveTime) {
	this.capacity = capacity;
    this.liveTime = liveTime;
    this.head = null;
    this.tail = null;
  }

 
  public void set(String key, T value) {
    if (cache.containsKey(key)) {
      DLinkedNode oldNode = cache.get(key);
      // Update the value
      if (oldNode.value != null) {
        oldNode.value.setValue(value);
        oldNode.value.setAccessTime(System.currentTimeMillis());  
      }
      removeNode(oldNode);
      insertHeadNode(oldNode);
    } else {
      DLinkedNode newNode = new DLinkedNode();
      newNode.key = key;
      newNode.value = new CacheObject<T>(value);
      if (cache.size() >= capacity) {
    	cache.remove(tail.key);
    	removeNode(tail);
    	insertHeadNode(newNode);
      } else {
    	insertHeadNode(newNode);
      }
      cache.put(key, newNode);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("LRU cache size " + cache.size());
    }
  }
  
  public T get(String key) {
	if (cache.containsKey(key)) {
	  DLinkedNode oldNode = cache.get(key);
		
	  /**
	   * If the value expired remove the entry
	   */
	  if (oldNode.value == null || oldNode.value.isExpired(liveTime)) {
        if (LOG.isDebugEnabled()) {
		  LOG.debug("Expiring cache entry " + key);
		}
		removeNode(oldNode);
		return null;
	  }
		
	  removeNode(oldNode);
	  oldNode.value.setAccessTime(System.currentTimeMillis());
	  insertHeadNode(oldNode);
	  return oldNode.value.getValue();
	} else {
	  return null;
	}
  }
  
  public boolean remove(String key) {
    if (cache.containsKey(key)) {
      DLinkedNode oldNode = cache.get(key);
	  removeNode(oldNode);
	  return true;
	} else {
      return false;
	}
  }


  public int getSize() {
	return cache.size();
  }
  
}
