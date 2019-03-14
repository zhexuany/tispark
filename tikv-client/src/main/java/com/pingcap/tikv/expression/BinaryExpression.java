package com.pingcap.tikv.expression;

public abstract class BinaryExpression implements Expression {
  protected Expression left;
  protected Expression right;

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }
}
