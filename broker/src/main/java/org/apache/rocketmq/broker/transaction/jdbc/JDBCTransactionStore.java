/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.transaction.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.transaction.TransactionRecord;
import org.apache.rocketmq.broker.transaction.TransactionStore;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// 它封装了对象数据库表t_transaction的相关操作, 其中记录了offset、group数据
public class JDBCTransactionStore implements TransactionStore {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    
    // 数据库驱动、url、用户名、用户密码
    private final JDBCTransactionStoreConfig jdbcTransactionStoreConfig;
    
    // 数据库连接对象
    private Connection connection;
    
    // 记录t_transaction表中offset行数
    private AtomicLong totalRecordsValue = new AtomicLong(0);

    // 创建JDBCTransactionStore对象
    public JDBCTransactionStore(JDBCTransactionStoreConfig jdbcTransactionStoreConfig) {
        this.jdbcTransactionStoreConfig = jdbcTransactionStoreConfig;
    }

    @Override
    public boolean open() {
    	// 加载com.mysql.jdbc.driver驱动类
        if (this.loadDriver()) {
        	// 从配置类中去获取用户名与用户密码
            Properties props = new Properties();
            props.put("user", jdbcTransactionStoreConfig.getJdbcUser());
            props.put("password", jdbcTransactionStoreConfig.getJdbcPassword());

            try {
            	// 获取MySql数据库连接对象
                this.connection =
                    DriverManager.getConnection(this.jdbcTransactionStoreConfig.getJdbcURL(), props);

                // 设置autoCommit为false,即
                this.connection.setAutoCommit(false);

                if (!this.computeTotalRecords()) {
                    return this.createDB();
                }

                return true;
            } catch (SQLException e) {
                log.info("Create JDBC Connection Exception", e);
            }
        }

        return false;
    }

    // 类加载器,去初始化com.mysql.jdbc.driver驱动类
    private boolean loadDriver() {
        try {
            Class.forName(this.jdbcTransactionStoreConfig.getJdbcDriverClass()).newInstance();
            log.info("Loaded the appropriate driver, {}",
                this.jdbcTransactionStoreConfig.getJdbcDriverClass());
            return true;
        } catch (Exception e) {
            log.info("Loaded the appropriate driver Exception", e);
        }

        return false;
    }

    // 从数据库中获取表t_transaction中的offset行数
    private boolean computeTotalRecords() {
        Statement statement = null;
        ResultSet resultSet = null;
        try {
        	// 创建Statement对象
            statement = this.connection.createStatement();

            // 统计t_transaction表中offset行数
            resultSet = statement.executeQuery("select count(offset) as total from t_transaction");
            
            if (!resultSet.next()) {
                log.warn("computeTotalRecords ResultSet is empty");
                return false;
            }

            // 设置offset行数
            this.totalRecordsValue.set(resultSet.getLong(1));
        } catch (Exception e) {
            log.warn("computeTotalRecords Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                	// 关闭statement对象
                    statement.close();
                } catch (SQLException e) {
                }
            }

            if (null != resultSet) {
                try {
                	// 关闭resultSet对象
                    resultSet.close();
                } catch (SQLException e) {
                }
            }
        }

        return true;
    }

    // 执行特定sql语句CREATE TABLE t_transaction 
    private boolean createDB() {
        Statement statement = null;
        try {
            statement = this.connection.createStatement();

            String sql = this.createTableSql();
            log.info("createDB SQL:\n {}", sql);
            // 执行建表语句
            statement.execute(sql);
            this.connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                	// 关闭statement对象
                    statement.close();
                } catch (SQLException e) {
                    log.warn("Close statement exception", e);
                }
            }
        }
    }

    // 获取transaction.sql中的建表语句
    private String createTableSql() {
    	// 获取transaction.sql文件的URL对象
        URL resource = JDBCTransactionStore.class.getClassLoader().getResource("transaction.sql");
        String fileContent = MixAll.file2String(resource);
        return fileContent;
    }

    @Override // 关闭数据库连接
    public void close() {
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (SQLException e) {
        }
    }

    @Override // 批量执行insert into t_transaction values (?, ?)语句
    public boolean put(List<TransactionRecord> trs) {
        PreparedStatement statement = null;
        try {
            this.connection.setAutoCommit(false);
            statement = this.connection.prepareStatement("insert into t_transaction values (?, ?)");
            for (TransactionRecord tr : trs) {
            	// SQL语句设置参数
                statement.setLong(1, tr.getOffset());
                statement.setString(2, tr.getProducerGroup());
                statement.addBatch(); // 批量执行
            }
            // 跑批执行Sql语句
            int[] executeBatch = statement.executeBatch();
            this.connection.commit();
            this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            if (null != statement) {
                try {
                	// 关闭statement对象
                    statement.close();
                } catch (SQLException e) {
                    log.warn("Close statement exception", e);
                }
            }
        }
    }

    // 计算rows数组总和
    private long updatedRows(int[] rows) {
        long res = 0;
        for (int i : rows) {
            res += i;
        }

        return res;
    }

    @Override // 批量执行DELETE FROM t_transaction WHERE offset = ?语句
    public void remove(List<Long> pks) {
        PreparedStatement statement = null;
        try {
        	// 设置autoCommit为false
            this.connection.setAutoCommit(false);
            // 删除offset
            statement = this.connection.prepareStatement("DELETE FROM t_transaction WHERE offset = ?");
            for (long pk : pks) {
            	// 设置参数
                statement.setLong(1, pk);
                statement.addBatch();
            }
            // 执行批量操作 int[] executeBatch
            statement.executeBatch();
            // 事务提交
            this.connection.commit();
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            if (null != statement) {
                try {
                	// 关闭statement对象
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        return null;
    }

    @Override // 获取表t_transaction表中的offset行数
    public long totalRecords() {
        return this.totalRecordsValue.get();
    }

    @Override // 0
    public long minPK() {
        return 0;
    }

    @Override // 0
    public long maxPK() {
        return 0;
    }
}
