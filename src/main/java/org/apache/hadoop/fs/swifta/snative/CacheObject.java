package org.apache.hadoop.fs.swifta.snative;

public class CacheObject<T> {

  private T value;
  private long accessTime;

  public CacheObject(T value) {
    this.value = value;
    this.accessTime = System.currentTimeMillis();
  }

  public T getValue() {
    return value;
  }
  
  public void setValue(T value) {
	this.value = value;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  public boolean isExpired(long liveTime) {
    return (this.accessTime + liveTime) > System.currentTimeMillis();
  }
}
