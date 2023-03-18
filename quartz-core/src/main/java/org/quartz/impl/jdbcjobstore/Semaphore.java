/* 
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.quartz.impl.jdbcjobstore;

import java.sql.Connection;

/**
 * An interface for providing thread/resource locking in order to protect
 * resources from being altered by multiple threads at the same time.
 * 
 * @author jhouse
 */
public interface Semaphore {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Interface.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * Grants a lock on the identified resource to the calling thread (blocking
     * until it is available).
     *
     * <p>
     *     将标识的资源上的锁授予调用线程（阻塞直到它可用）。
     *     SELECT * FROM EB_QRTO_LOCKS WHERE SCHED_NAME = 'EB_ORD_SCHEDULER' AND LOCK_NAME = ? FOR UPDATE
     *     [TRIGGER_ACCESS]
     * </p>
     * 
     * @param conn Database connection used to establish lock.  Can be null if
     * <code>{@link #requiresConnection()}</code> returns false.
     * 
     * @return true if the lock was obtained.
     */
    boolean obtainLock(Connection conn, String lockName) throws LockException;

    /**
     * Release the lock on the identified resource if it is held by the calling thread.
     *
     * 释放已识别资源的锁（如果该资源由调用持有）线程。
     */
    void releaseLock(String lockName) throws LockException;

    /**
     * Whether this Semaphore implementation requires a database connection for
     * its lock management operations.
     *
     * 此信号量实现是否需要数据库连接来执行锁定管理操作。
     * 
     * @see #obtainLock(Connection, String)
     * @see #releaseLock(String)
     */
    boolean requiresConnection();
}
