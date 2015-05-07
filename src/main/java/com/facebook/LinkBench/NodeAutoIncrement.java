package com.facebook.LinkBench;

/**
 * This is a singleton sequence generator for databases that
 * do not support auto incrementing integer columns
 * (such as MongoDB)
 * 
 * @author dbennett
 *
 */

public class NodeAutoIncrement {
  private static NodeAutoIncrement instance = new NodeAutoIncrement();

  private volatile long next = 1;

  private NodeAutoIncrement() {
    // block instantiation
  }

  public static NodeAutoIncrement getInstance() {
      return instance;
  }

  public synchronized long getNextSequence() {
      return next++;
  }
  
  public synchronized NodeAutoIncrement setNext(long n) {
    next=n;
    return this;
  }
  
}