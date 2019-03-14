package com.pingcap.tikv.expression;

import java.util.Objects;

public abstract class UnaryExpression implements Expression {
  protected Expression child;

  public Expression getChild() {
    return this.child;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(child);
  }
}
