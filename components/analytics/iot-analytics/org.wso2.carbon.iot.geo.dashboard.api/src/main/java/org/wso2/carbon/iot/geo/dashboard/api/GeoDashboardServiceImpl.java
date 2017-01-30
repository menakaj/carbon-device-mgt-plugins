package org.wso2.carbon.iot.geo.dashboard.api;


import org.apache.axis2.AxisFault;
import org.apache.axis2.client.Options;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.context.ServiceContext;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.wso2.carbon.analytics.api.AnalyticsDataAPI;
import org.wso2.carbon.analytics.api.AnalyticsDataAPIUtil;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse;
import org.wso2.carbon.analytics.dataservice.commons.SearchResultEntry;
import org.wso2.carbon.analytics.dataservice.commons.SortByField;
import org.wso2.carbon.analytics.dataservice.commons.SortType;
import org.wso2.carbon.analytics.dataservice.commons.exception.AnalyticsIndexException;
import org.wso2.carbon.analytics.datasource.commons.AnalyticsSchema;
import org.wso2.carbon.analytics.datasource.commons.ColumnDefinition;
import org.wso2.carbon.analytics.datasource.commons.Record;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsException;
import org.wso2.carbon.authenticator.stub.AuthenticationAdminStub;
import org.wso2.carbon.authenticator.stub.LoginAuthenticationExceptionException;
import org.wso2.carbon.context.CarbonContext;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.iot.geo.dashboard.api.constants.GeoDashboardConstants;
import org.wso2.carbon.iot.geo.dashboard.api.util.APIUtil;
import org.wso2.carbon.event.processor.stub.EventProcessorAdminServiceStub;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GeoDashboardServiceImpl implements GeoDashboardService {

    private static Log log = LogFactory.getLog(GeoDashboardServiceImpl.class);
    private AuthenticationAdminStub authenticationAdminStub = null;

    /**
     * This will retrieve stats from given table for given time range from Data Analytics Server(DAS)
     *
     * @param tableName datasource name
     * @param timeFrom  starting time
     * @param timeTo    ending time
     * @param count     maximum number of elements need to be retrieved
     * @return sensor data which were retrieved from DAS
     */
    @Path("geo-dashboard/stats/by-range")
    @GET
    @Produces({"application/json"})
    public Response getStatsByRange(@QueryParam("tableName") String tableName, @QueryParam("timeFrom") long timeFrom
            , @QueryParam("timeTo") long timeTo, @QueryParam("count") int count) {
        String fromDate = String.valueOf(timeFrom);
        String toDate = String.valueOf(timeTo);
        String query = GeoDashboardConstants.DEVICE_META_INFO_TIME + " :[" + fromDate + " TO " + toDate + "]";
        int eventCount;
        List<SortByField> sortByFields = new ArrayList<>();
        SortByField sortByField = new SortByField("time", SortType.ASC);
        sortByFields.add(sortByField);
        try {
            int tenantId = CarbonContext.getThreadLocalCarbonContext().getTenantId();
            AnalyticsDataAPI analyticsDataAPI = APIUtil.getAnalyticsDataAPI();
            eventCount = analyticsDataAPI.searchCount(tenantId, tableName, query);
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode rootNode = mapper.createObjectNode();
            ArrayNode dataSet = mapper.createArrayNode();
            if (eventCount == 0) {
                rootNode.put(GeoDashboardConstants.RESPONSE_STATUS, GeoDashboardConstants.SUCCESS_MESSAGE);
                rootNode.put(GeoDashboardConstants.RESPONSE_MESSAGE, dataSet);
                return Response.ok(rootNode.toString()).build();
            }
            if (eventCount > count) {
                eventCount = count;
            }
            List<SearchResultEntry> resultEntries = analyticsDataAPI.search(tenantId, tableName, query, 0, eventCount
                    , sortByFields);
            List<String> recordIds = getRecordIds(resultEntries);
            AnalyticsDataResponse response = analyticsDataAPI.get(tenantId, tableName, 1, null, recordIds);
            List<Record> records = AnalyticsDataAPIUtil.listRecords(analyticsDataAPI, response);
            for (Object recodeObject : records) {
                Record record = (Record) recodeObject;
                Map<String, Object> row = record.getValues();
                Iterator<Map.Entry<String, Object>> entries = row.entrySet().iterator();
                ObjectNode sonsorData = mapper.createObjectNode();
                while (entries.hasNext()) {
                    Map.Entry<String, Object> entry = entries.next();
                    sonsorData.put(entry.getKey(), "" + entry.getValue());
                }
                dataSet.add(sonsorData);
            }
            rootNode.put(GeoDashboardConstants.RESPONSE_STATUS, GeoDashboardConstants.SUCCESS_MESSAGE);
            rootNode.put(GeoDashboardConstants.RESPONSE_MESSAGE, dataSet);
            return Response.ok(rootNode.toString()).build();
        } catch (AnalyticsIndexException e) {
            String errorMsg = "Error on retrieving stats on table " + tableName + " with query " + query;
            log.error(errorMsg);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()).entity(errorMsg).build();
        } catch (AnalyticsException e) {
            log.error("Error while retrieving data from DAS, " + e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }
    }

    @Path("geo-dashboard/remove/executionplan")
    @POST
    @Produces({"application/json"})
    public Response removeExecutionPlan(String executionPlanName) {
        String eventProcessorAdminServiceWSUrl = APIUtil.getDASEndpoint();
        try {
            EventProcessorAdminServiceStub eventprocessorStub =
                    new EventProcessorAdminServiceStub(eventProcessorAdminServiceWSUrl);
            String sessionCookie = getSessionCookie();
            ServiceClient eventsProcessorServiceClient = eventprocessorStub._getServiceClient();
            Options eventProcessorOption = eventsProcessorServiceClient.getOptions();
            eventProcessorOption.setManageSession(true);
            eventProcessorOption.setProperty(HTTPConstants.COOKIE_STRING, sessionCookie);
            eventprocessorStub.undeployActiveExecutionPlan(executionPlanName);
            return Response.ok().build();
        } catch (AxisFault axisFault) {
            log.error(axisFault);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (RemoteException e) {
            log.error(e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (Exception e) {
            log.error(e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Path("geo-dashboard/deploy/executionplain")
    @POST
    @Produces({"application/json"})
    public Response deployExecutionPlan(String executionPlanName) {
        String eventProcessorAdminServiceWSUrl = APIUtil.getDASEndpoint();
        try {
            EventProcessorAdminServiceStub eventprocessorStub =
                    new EventProcessorAdminServiceStub(eventProcessorAdminServiceWSUrl);
            String sessionCookie = getSessionCookie();
            ServiceClient eventsProcessorServiceClient = eventprocessorStub._getServiceClient();
            Options eventProcessorOption = eventsProcessorServiceClient.getOptions();
            eventProcessorOption.setManageSession(true);
            eventProcessorOption.setProperty(HTTPConstants.COOKIE_STRING, sessionCookie);
            eventprocessorStub.deployExecutionPlan(executionPlanName);
            return Response.ok().build();
        } catch (AxisFault axisFault) {
            log.error(axisFault);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (RemoteException e) {
            log.error(e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (Exception e) {
            log.error(e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Get schema definition of given datasource
     *
     * @param dataSource data source name
     * @return schema definition
     */
    @Path("geo-dashboard/stats/schema")
    @GET
    @Produces({"application/json"})
    public Response getSchema(@QueryParam("dataSource") String dataSource) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = mapper.createObjectNode();
        ObjectNode columnsNodes = mapper.createObjectNode();
        ArrayNode primaryKeyNodes = mapper.createArrayNode();
        ObjectNode dataSourcesList = mapper.createObjectNode();
        Response tenantId;
        try {
            int tenantId1 = CarbonContext.getThreadLocalCarbonContext().getTenantId();
            AnalyticsDataAPI analyticsDataAPI = APIUtil.getAnalyticsDataAPI();
            AnalyticsSchema tableSchema = analyticsDataAPI.getTableSchema(tenantId1, dataSource);
            Map<String, ColumnDefinition> columns = tableSchema.getColumns();
            if (columns == null) {
                log.error("Table doesn't contains eny columns");
                rootNode.put(GeoDashboardConstants.RESPONSE_STATUS, GeoDashboardConstants.FAILURE_MESSAGE);
                rootNode.put(GeoDashboardConstants.RESPONSE_MESSAGE, "Table doesn't contains eny columns");
                return Response.status(Response.Status.NO_CONTENT).entity(rootNode).build();
            }
            for (String column : columns.keySet()) {
                ObjectNode key = mapper.createObjectNode();
                ColumnDefinition columnDefinition = columns.get(column);
                key.put(GeoDashboardConstants.SCHEMA_DEFINITION_COLUMN_NAME, columnDefinition.getName());
                key.put(GeoDashboardConstants.SCHEMA_DEFINITION_COLUMN_TYPE
                        , columnDefinition.getType().toString());
                key.put(GeoDashboardConstants.SCHEMA_DEFINITION_COLUMN_INDEXED, columnDefinition.isIndexed());
                key.put(GeoDashboardConstants.SCHEMA_DEFINITION_COLUMN_SCOREPARAM
                        , columnDefinition.isScoreParam());
                dataSourcesList.put(column, key);
            }
            List<String> primaryKeys = tableSchema.getPrimaryKeys();
            for (String primaryKey : primaryKeys) {
                primaryKeyNodes.add(primaryKey);
            }
            rootNode.put(GeoDashboardConstants.RESPONSE_STATUS, GeoDashboardConstants.SUCCESS_MESSAGE);
            columnsNodes.put(GeoDashboardConstants.SCHEMA_DEFINITION_COLUMN_PRIMARYKEYS, primaryKeyNodes);
            columnsNodes.put(GeoDashboardConstants.SCHEMA_DEFINITION_COLUMNS, dataSourcesList);
            rootNode.put(GeoDashboardConstants.RESPONSE_MESSAGE, columnsNodes);
            return Response.ok(rootNode.toString()).build();
        } catch (AnalyticsException e) {
            log.error("Error while retrieving data from DAS, " + e);
            rootNode.put(GeoDashboardConstants.RESPONSE_STATUS, GeoDashboardConstants.FAILURE_MESSAGE);
            rootNode.put(GeoDashboardConstants.RESPONSE_MESSAGE, e.getMessage());
            tenantId = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rootNode).build();
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }
        return tenantId;
    }

    @Path("geo-dashboard/get/geo-fences/details")
    @GET
    @Produces({"application/json"})
    public Response getGeoFenceDetails(@QueryParam("executionPlanName") String executionPlanName,
                              @QueryParam("deviceId") String deviceId){
        //TODO
        return null;
    }

    @Path("geo-dashboard/get/geo-location-history")
    @GET
    @Produces({"application/json"})
    public Response getGeoLocationHistory(@QueryParam("deviceId") String deviceId){
        //TODO
        return null;
    }

    private void setAuthenticatorClient(String backendUrl) throws AxisFault {
        try {
            String serviceName = GeoDashboardConstants.AUTHENTICATION_ADMIN_SERVICE;
            String endPoint = backendUrl + serviceName;
            if (log.isDebugEnabled()) {
                log.debug("EndPoint" + endPoint);
            }
            authenticationAdminStub = new AuthenticationAdminStub(endPoint);
        } catch (AxisFault var5) {
            log.info("authenticationAdminStub initialization fails");
            throw new AxisFault("authenticationAdminStub initialization fails");
        }
    }

    private String getSessionCookie() throws Exception {
        return login(GeoDashboardConstants.DAS_ADMIN_USERNAME, GeoDashboardConstants.DAS_ADMIN_PASSWORD,
                GeoDashboardConstants.DAS_HOST);
    }

    private String login(String userName, String password, String host) throws LoginAuthenticationExceptionException,
            RemoteException {
        if (authenticationAdminStub == null) {
            setAuthenticatorClient(APIUtil.getDASEndpoint());
        }
        Boolean loginStatus = this.authenticationAdminStub.login(userName, password, host);
        if (!loginStatus) {
            throw new LoginAuthenticationExceptionException("Login Unsuccessful. Return false as a login status by " +
                    "Server");
        } else {
            ServiceContext serviceContext = this.authenticationAdminStub._getServiceClient().getLastOperationContext().
                    getServiceContext();
            String sessionCookie = (String) serviceContext.getProperty("Cookie");
            if (log.isDebugEnabled()) {
                log.debug("Login Successful to DAS");
                log.debug("SessionCookie :" + sessionCookie);
            }
            return sessionCookie;
        }
    }

    private List<String> getRecordIds(List<SearchResultEntry> searchResults) {
        ArrayList<String> ids = new ArrayList<>();
        for (SearchResultEntry searchResult : searchResults) {
            ids.add(searchResult.getId());
        }
        return ids;
    }
}
