/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.device.mgt.mobile.dao.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.device.mgt.common.DeviceManagementException;
import org.wso2.carbon.device.mgt.mobile.util.MobileDeviceManagementSchemaInitializer;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

/**
 * Utility method required by MobileDeviceManagement DAO classes.
 */
public class MobileDeviceManagementDAOUtil {

    private static final Log log = LogFactory.getLog(MobileDeviceManagementDAOUtil.class);

    public static DataSource lookupDataSource(String dataSourceName,
                                              final Hashtable<Object, Object> jndiProperties)
            throws DeviceManagementException {
        try {
            if (jndiProperties == null || jndiProperties.isEmpty()) {
                return (DataSource) InitialContext.doLookup(dataSourceName);
            }
            final InitialContext context = new InitialContext(jndiProperties);
            return (DataSource) context.lookup(dataSourceName);
        } catch (Exception e) {
            String msg = "Error in looking up data source: " + e.getMessage();
            log.error(msg, e);
            throw new DeviceManagementException(msg, e);
        }
    }

    public static void cleanupResources(Connection conn, PreparedStatement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Error occurred while closing result set", e);
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.warn("Error occurred while closing prepared statement", e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.warn("Error occurred while closing database connection", e);
            }
        }
    }

    /**
     * Creates the mobile device management schema.
     *
     * @param dataSource Mobile data source
     */
    public static void setupMobileDeviceManagementSchema(DataSource dataSource) throws
            DeviceManagementException {
        MobileDeviceManagementSchemaInitializer initializer =
                new MobileDeviceManagementSchemaInitializer(dataSource);
        log.info("Initializing mobile device management repository database schema");
        try {
            initializer.createRegistryDatabase();
        } catch (Exception e) {
            throw new DeviceManagementException("Error occurred while initializing Mobile Device " +
                                                "Management database schema", e);
        }
    }

}
