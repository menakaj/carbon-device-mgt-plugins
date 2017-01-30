/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.iot.geo.dashboard.api.constants;

/**
 * Device type specific constants which includes all transport protocols configurations,
 * stream definition and device specific constants
 */
public class GeoDashboardConstants {

    public final static String DAS_ADMIN_USERNAME = "admin";
    public final static String DAS_ADMIN_PASSWORD = "admin";
    public final static String DAS_HOST= "localhost";
    public final static String EVENT_PROCESSOR_SERVICE = "/services/EventProcessorAdminService";
    public final static String AUTHENTICATION_ADMIN_SERVICE = "AuthenticationAdmin";


    public static final String RESPONSE_STATUS = "status";
    public static final String RESPONSE_MESSAGE = "message";
    public static final String SUCCESS_MESSAGE = "success";
    public static final String FAILURE_MESSAGE = "failed";
    public static final String SCHEMA_DEFINITION_COLUMN_NAME = "name";
    public static final String SCHEMA_DEFINITION_COLUMN_TYPE = "type";
    public static final String SCHEMA_DEFINITION_COLUMN_INDEXED = "indexed";
    public static final String SCHEMA_DEFINITION_COLUMN_SCOREPARAM = "scoreParam";
    public static final String SCHEMA_DEFINITION_COLUMNS = "columns";
    public static final String SCHEMA_DEFINITION_COLUMN_PRIMARYKEYS = "primaryKeys";
    public final static String DEVICE_META_INFO_TIME = "meta_time";
}

