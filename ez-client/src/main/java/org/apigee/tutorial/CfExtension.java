package org.apigee.tutorial;

/**
 * @author zznate
 */
public class CfExtension {

  private final ExtType type;
  private String name;

  CfExtension(ExtType type) {
    this.type = type;
  }

  public CfExtension name(String name) {
    this.name = name;
    return this;
  }

  enum ExtType {
    INDEX,
    COUNTER;
  }
}
