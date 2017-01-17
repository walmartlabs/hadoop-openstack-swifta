package org.apache.hadoop.fs.swifta.snative;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LRU Cache for reduce HEAD requests.
 *
 */
public class LRUCache<T> {
  class DLinkedNode {
    String key;
    T value;
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
   * Remove an existing node from the linked list.
   */
  private void removeNode(DLinkedNode node) {
    DLinkedNode pre = node.pre;
    DLinkedNode post = node.post;
    if (pre != null) {
      pre.post = post;
    }
    if (post != null) {
      post.pre = pre;
    }
  }

  /**
   * Move certain node in between to the head.
   */
  private void moveToHead(DLinkedNode node) {
    this.removeNode(node);
    this.addNode(node);
  }

  // pop the current tail.
  private DLinkedNode popTail() {
    DLinkedNode res = tail.pre;
    this.removeNode(res);
    return res;
  }

  private Map<String, DLinkedNode> cache = new ConcurrentHashMap<String, DLinkedNode>();
  private int count;
  private int capacity;
  private DLinkedNode head, tail;

  public LRUCache(int capacity) {
    this.count = 0;
    this.capacity = capacity;

    head = new DLinkedNode();
    head.pre = null;

    tail = new DLinkedNode();
    tail.post = null;

    head.post = tail;
    tail.pre = head;
  }

  public T get(String key) {

    DLinkedNode node = cache.get(key);
    if (node == null) {
      return null; // should raise exception here.
    }

    // move the accessed node to the head;
    this.moveToHead(node);

    return node.value;
  }

  public boolean remove(String key) {

    DLinkedNode node = cache.get(key);
    if (node == null) {
      return Boolean.FALSE; // should raise exception here.
    }
    this.removeNode(node);
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
      newNode.value = value;

      this.cache.put(key, newNode);
      this.addNode(newNode);

      ++count;

      if (count > capacity) {
        // pop the tail
        DLinkedNode tail = this.popTail();
        if (tail != null && tail.key != null)
          this.cache.remove(tail.key);
        --count;
      }
    } else {
      // update the value.
      node.value = value;
      this.moveToHead(node);
    }

  }
}
