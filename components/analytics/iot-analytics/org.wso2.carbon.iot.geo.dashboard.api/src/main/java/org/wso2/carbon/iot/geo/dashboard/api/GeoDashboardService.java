package org.wso2.carbon.iot.geo.dashboard.api;


import javax.ws.rs.*;
import javax.ws.rs.core.Response;

interface GeoDashboardService {
    /**
     * Get schema definition of given datasource
     *
     * @param dataSource data source name
     * @return schema definition
     */
    @Path("geo-dashboard/stats/schema")
    @GET
    @Produces({"application/json"})
    Response getSchema(@QueryParam("dataSource") String dataSource);

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
    Response getStatsByRange(@QueryParam("tableName") String tableName, @QueryParam("timeFrom") long timeFrom
            , @QueryParam("timeTo") long timeTo, @QueryParam("count") int count);

    /**
     *
     * @param executionPlanName
     * @return
     */
    @Path("geo-dashboard/remove/executionplain")
    @POST
    @Produces({"application/json"})
    Response removeExecutionPlan(@QueryParam("executionPlanName") String executionPlanName);

    /**
     *
     * @param executionPlanName
     * @return
     */
    @Path("geo-dashboard/deploy/executionplain")
    @POST
    @Produces({"application/json"})
    Response deployExecutionPlan(@QueryParam("executionPlanName") String executionPlanName);

    /**
     *
     * @param executionPlanName
     * @param deviceId
     * @return
     */
    @Path("geo-dashboard/get/geo-fences/details")
    @GET
    @Produces({"application/json"})
    Response getGeoFenceDetails(@QueryParam("executionPlanName") String executionPlanName,
                                @QueryParam("deviceId") String deviceId);

    /**
     *
     * @param deviceId
     * @return
     */
    @Path("geo-dashboard/get/geo-location-history")
    @GET
    @Produces({"application/json"})
    Response getGeoLocationHistory(@QueryParam("deviceId") String deviceId);


}
