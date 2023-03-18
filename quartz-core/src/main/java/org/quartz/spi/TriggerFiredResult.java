package org.quartz.spi;

import org.quartz.impl.jdbcjobstore.JobStoreSupport;

import java.sql.Connection;

/**
 * @author lorban
 */
public class TriggerFiredResult {

  /**
   * 触发器点火包装类
   */
  private TriggerFiredBundle triggerFiredBundle;

  /**
   * 如果在执行生成 TriggerFiredBundle 对象的时候，发生了异常，将异常存储到此属性中。 JobStoreSupport#triggerFired(Connection, OperableTrigger)
   */
  private Exception exception;

  public TriggerFiredResult(TriggerFiredBundle triggerFiredBundle) {
    this.triggerFiredBundle = triggerFiredBundle;
  }

  public TriggerFiredResult(Exception exception) {
    this.exception = exception;
  }

  public TriggerFiredBundle getTriggerFiredBundle() {
    return triggerFiredBundle;
  }

  public Exception getException() {
    return exception;
  }
}
