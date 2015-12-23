/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.carbon.device.mgt.iot.virtualfirealarm.service;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.apimgt.annotations.api.API;
import org.wso2.carbon.apimgt.annotations.device.DeviceType;
import org.wso2.carbon.apimgt.annotations.device.feature.Feature;
import org.wso2.carbon.apimgt.webapp.publisher.KeyGenerationUtil;
import org.wso2.carbon.certificate.mgt.core.dto.SCEPResponse;
import org.wso2.carbon.certificate.mgt.core.exception.KeystoreException;
import org.wso2.carbon.certificate.mgt.core.service.CertificateManagementService;
import org.wso2.carbon.device.mgt.common.Device;
import org.wso2.carbon.device.mgt.common.DeviceIdentifier;
import org.wso2.carbon.device.mgt.common.DeviceManagementException;
import org.wso2.carbon.device.mgt.common.EnrolmentInfo;
import org.wso2.carbon.device.mgt.iot.DeviceManagement;
import org.wso2.carbon.device.mgt.iot.DeviceValidator;
import org.wso2.carbon.device.mgt.iot.apimgt.AccessTokenInfo;
import org.wso2.carbon.device.mgt.iot.apimgt.TokenClient;
import org.wso2.carbon.device.mgt.iot.controlqueue.mqtt.MqttConfig;
import org.wso2.carbon.device.mgt.iot.controlqueue.xmpp.XmppAccount;
import org.wso2.carbon.device.mgt.iot.controlqueue.xmpp.XmppConfig;
import org.wso2.carbon.device.mgt.iot.controlqueue.xmpp.XmppServerClient;
import org.wso2.carbon.device.mgt.iot.exception.AccessTokenException;
import org.wso2.carbon.device.mgt.iot.exception.DeviceControllerException;
import org.wso2.carbon.device.mgt.iot.sensormgt.SensorDataManager;
import org.wso2.carbon.device.mgt.iot.sensormgt.SensorRecord;
import org.wso2.carbon.device.mgt.iot.transport.TransportHandlerException;
import org.wso2.carbon.device.mgt.iot.util.ZipArchive;
import org.wso2.carbon.device.mgt.iot.util.ZipUtil;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.plugin.constants.VirtualFireAlarmConstants;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.dto.DeviceJSON;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.exception.VirtualFireAlarmException;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.transport.VirtualFireAlarmMQTTConnector;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.transport.VirtualFireAlarmXMPPConnector;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.util.VerificationManager;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.util.VirtualFireAlarmServiceUtils;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.util.scep.ContentType;
import org.wso2.carbon.device.mgt.iot.virtualfirealarm.service.util.scep.SCEPOperation;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@API(name = "virtual_firealarm", version = "1.0.0", context = "/virtual_firealarm")
@DeviceType(value = "virtual_firealarm")
public class VirtualFireAlarmService {

    private static Log log = LogFactory.getLog(VirtualFireAlarmService.class);

    //TODO; replace this tenant domain
    private static final String SUPER_TENANT = "carbon.super";

    @Context  //injected response proxy supporting multiple thread
    private HttpServletResponse response;

    public static final String XMPP_PROTOCOL = "XMPP";
    public static final String HTTP_PROTOCOL = "HTTP";
    public static final String MQTT_PROTOCOL = "MQTT";

    private VerificationManager verificationManager;
    private VirtualFireAlarmMQTTConnector virtualFireAlarmMQTTConnector;
    private VirtualFireAlarmXMPPConnector virtualFireAlarmXMPPConnector;
    private ConcurrentHashMap<String, String> deviceToIpMap = new ConcurrentHashMap<>();

    /**
     * @param verificationManager
     */
    public void setVerificationManager(
            VerificationManager verificationManager) {
        this.verificationManager = verificationManager;
        verificationManager.initVerificationManager();
    }

    /**
     * @param virtualFireAlarmXMPPConnector
     */
    public void setVirtualFireAlarmXMPPConnector(
            final VirtualFireAlarmXMPPConnector virtualFireAlarmXMPPConnector) {
        this.virtualFireAlarmXMPPConnector = virtualFireAlarmXMPPConnector;

        if (XmppConfig.getInstance().isEnabled()) {
            Runnable xmppStarter = new Runnable() {
                @Override
                public void run() {
                    virtualFireAlarmXMPPConnector.initConnector();
                    virtualFireAlarmXMPPConnector.connect();
                }
            };

            Thread xmppStarterThread = new Thread(xmppStarter);
            xmppStarterThread.setDaemon(true);
            xmppStarterThread.start();
        } else {
            log.warn("MQTT disabled in 'devicemgt-config.xml'. Hence, VirtualFireAlarmMQTTConnector not started.");
        }
    }

    /**
     * @param virtualFireAlarmMQTTConnector
     */
    public void setVirtualFireAlarmMQTTConnector(
            final VirtualFireAlarmMQTTConnector virtualFireAlarmMQTTConnector) {
        this.virtualFireAlarmMQTTConnector = virtualFireAlarmMQTTConnector;
        if (MqttConfig.getInstance().isEnabled()) {
            virtualFireAlarmMQTTConnector.connect();
        } else {
            log.warn("XMPP disabled in 'devicemgt-config.xml'. Hence, VirtualFireAlarmXMPPConnector not started.");
        }
    }

    /**
     * @return
     */
    public VerificationManager getVerificationManager() {
        return verificationManager;
    }

    /**
     * @return
     */
    public VirtualFireAlarmXMPPConnector getVirtualFireAlarmXMPPConnector() {
        return virtualFireAlarmXMPPConnector;
    }

    /**
     * @return
     */
    public VirtualFireAlarmMQTTConnector getVirtualFireAlarmMQTTConnector() {
        return virtualFireAlarmMQTTConnector;
    }

    /*	---------------------------------------------------------------------------------------
                    Device specific APIs - Control APIs + Data-Publishing APIs
            Also contains utility methods required for the execution of these APIs
         ---------------------------------------------------------------------------------------	*/

    /**
     * @param owner
     * @param deviceId
     * @param deviceIP
     * @param devicePort
     * @param response
     * @param request
     * @return
     */
    @Path("controller/register/{owner}/{deviceId}/{ip}/{port}")
    @POST
    public String registerDeviceIP(@PathParam("owner") String owner,
                                   @PathParam("deviceId") String deviceId,
                                   @PathParam("ip") String deviceIP,
                                   @PathParam("port") String devicePort,
                                   @Context HttpServletResponse response,
                                   @Context HttpServletRequest request) {

        //TODO:: Need to get IP from the request itself
        String result;

        log.info("Got register call from IP: " + deviceIP + " for Device ID: " + deviceId +
                         " of owner: " + owner);

        String deviceHttpEndpoint = deviceIP + ":" + devicePort;
        deviceToIpMap.put(deviceId, deviceHttpEndpoint);

        result = "Device-IP Registered";
        response.setStatus(Response.Status.OK.getStatusCode());

        if (log.isDebugEnabled()) {
            log.debug(result);
        }

        return result;
    }

    /*    Service to switch "ON" and "OFF" the Virtual FireAlarm bulb
           Called by an external client intended to control the Virtual FireAlarm bulb */

    /**
     * @param owner
     * @param deviceId
     * @param protocol
     * @param state
     * @param response
     */
    @Path("controller/buzzer")
    @POST
    @Feature( code="buzzer", name="Buzzer On / Off", type="operation",
            description="Switch on/off Virtual Fire Alarm Buzzer. (On / Off)")
    public void switchBulb(@HeaderParam("owner") String owner,
                           @HeaderParam("deviceId") String deviceId,
                           @HeaderParam("protocol") String protocol,
                           @FormParam("state") String state,
                           @Context HttpServletResponse response) {

        try {
            DeviceValidator deviceValidator = new DeviceValidator();
            if (!deviceValidator.isExist(owner, SUPER_TENANT, new DeviceIdentifier(deviceId,
                                                                                   VirtualFireAlarmConstants
                                                                                           .DEVICE_TYPE))) {
                response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
                return;
            }
        } catch (DeviceManagementException e) {
            log.error("DeviceValidation Failed for deviceId: " + deviceId + " of user: " + owner);
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            return;
        }

        String switchToState = state.toUpperCase();

        if (!switchToState.equals(VirtualFireAlarmConstants.STATE_ON) && !switchToState.equals(
                VirtualFireAlarmConstants.STATE_OFF)) {
            log.error("The requested state change shoud be either - 'ON' or 'OFF'");
            response.setStatus(Response.Status.BAD_REQUEST.getStatusCode());
            return;
        }

        String protocolString = protocol.toUpperCase();
        String callUrlPattern = VirtualFireAlarmConstants.BULB_CONTEXT + switchToState;

        if (log.isDebugEnabled()) {
            log.debug("Sending request to switch-bulb of device [" + deviceId + "] via " +
                              protocolString);
        }

        try {
            switch (protocolString) {
                case HTTP_PROTOCOL:
                    String deviceHTTPEndpoint = deviceToIpMap.get(deviceId);
                    if (deviceHTTPEndpoint == null) {
                        response.setStatus(Response.Status.PRECONDITION_FAILED.getStatusCode());
                        return;
                    }

                    VirtualFireAlarmServiceUtils.sendCommandViaHTTP(deviceHTTPEndpoint, callUrlPattern, true);
                    break;

                case MQTT_PROTOCOL:
                    String mqttResource = VirtualFireAlarmConstants.BULB_CONTEXT.replace("/", "");
                    virtualFireAlarmMQTTConnector.publishDeviceData(owner, deviceId, mqttResource, switchToState);
                    break;

                case XMPP_PROTOCOL:
                    String xmppResource = VirtualFireAlarmConstants.BULB_CONTEXT.replace("/", "");
                    virtualFireAlarmXMPPConnector.publishDeviceData(owner, deviceId, xmppResource, switchToState);
                    break;

                default:
                    response.setStatus(Response.Status.NOT_ACCEPTABLE.getStatusCode());
                    return;
            }
        } catch (DeviceManagementException | TransportHandlerException e) {
            log.error("Failed to send switch-bulb request to device [" + deviceId + "] via " + protocolString);
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
            return;
        }

        response.setStatus(Response.Status.OK.getStatusCode());
    }


    /**
     * @param owner
     * @param deviceId
     * @param protocol
     * @param response
     * @return
     */
    @Path("controller/readhumidity")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Feature(code = "readhumidity", name = "Humidity", type = "monitor",
            description = "Read Humidity Readings from Virtual Fire Alarm")
    public SensorRecord requestHumidity(@HeaderParam("owner") String owner,
                                        @HeaderParam("deviceId") String deviceId,
                                        @HeaderParam("protocol") String protocol,
                                        @Context HttpServletResponse response) {
        SensorRecord sensorRecord = null;
        DeviceValidator deviceValidator = new DeviceValidator();
        try {
            if (!deviceValidator.isExist(owner, SUPER_TENANT, new DeviceIdentifier(
                    deviceId, VirtualFireAlarmConstants.DEVICE_TYPE))) {
                response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
            }
        } catch (DeviceManagementException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        String protocolString = protocol.toUpperCase();

        if (log.isDebugEnabled()) {
            log.debug("Sending request to read humidity value of device [" + deviceId + "] via " + protocolString);
        }

        try {
            switch (protocolString) {
                case HTTP_PROTOCOL:
                    String deviceHTTPEndpoint = deviceToIpMap.get(deviceId);
                    if (deviceHTTPEndpoint == null) {
                        response.setStatus(Response.Status.PRECONDITION_FAILED.getStatusCode());
                    }

                    String humidityValue = VirtualFireAlarmServiceUtils.sendCommandViaHTTP(deviceHTTPEndpoint,
                                                                                           VirtualFireAlarmConstants.HUMIDITY_CONTEXT,
                                                                                           false);
                    SensorDataManager.getInstance().setSensorRecord(deviceId,
                                                                    VirtualFireAlarmConstants.SENSOR_TEMP,
                                                                    humidityValue,
                                                                    Calendar.getInstance().getTimeInMillis());
                    break;

                case MQTT_PROTOCOL:
                    String mqttResource = VirtualFireAlarmConstants.HUMIDITY_CONTEXT.replace("/", "");
                    virtualFireAlarmMQTTConnector.publishDeviceData(owner, deviceId, mqttResource, "");
                    break;

                case XMPP_PROTOCOL:
                    String xmppResource = VirtualFireAlarmConstants.HUMIDITY_CONTEXT.replace("/", "");
                    virtualFireAlarmXMPPConnector.publishDeviceData(owner, deviceId, xmppResource, "");
                    break;

                default:
                    response.setStatus(Response.Status.NOT_ACCEPTABLE.getStatusCode());
            }
            sensorRecord = SensorDataManager.getInstance().getSensorRecord(deviceId,
                                                                           VirtualFireAlarmConstants.SENSOR_HUMIDITY);
        } catch (DeviceManagementException | DeviceControllerException | TransportHandlerException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response.setStatus(Response.Status.OK.getStatusCode());
        return sensorRecord;
    }

    /**
     * @param owner
     * @param deviceId
     * @param protocol
     * @param response
     * @return
     */
    @Path("controller/readtemperature")
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Feature( code="readtemperature", name="Temperature", type="monitor",
            description="Request Temperature reading from Virtual Fire Alarm")
    public SensorRecord requestTemperature(@HeaderParam("owner") String owner,
                                           @HeaderParam("deviceId") String deviceId,
                                           @HeaderParam("protocol") String protocol,
                                           @Context HttpServletResponse response) {
        SensorRecord sensorRecord = null;

        DeviceValidator deviceValidator = new DeviceValidator();
        try {
            if (!deviceValidator.isExist(owner, SUPER_TENANT,
                                         new DeviceIdentifier(deviceId, VirtualFireAlarmConstants.DEVICE_TYPE))) {
                response.setStatus(Response.Status.UNAUTHORIZED.getStatusCode());
            }
        } catch (DeviceManagementException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        String protocolString = protocol.toUpperCase();

        if (log.isDebugEnabled()) {
            log.debug("Sending request to read virtual-firealarm-temperature of device " +
                              "[" + deviceId + "] via " + protocolString);
        }

        try {
            switch (protocolString) {
                case HTTP_PROTOCOL:
                    String deviceHTTPEndpoint = deviceToIpMap.get(deviceId);
                    if (deviceHTTPEndpoint == null) {
                        response.setStatus(Response.Status.PRECONDITION_FAILED.getStatusCode());
                    }

                    String temperatureValue = VirtualFireAlarmServiceUtils.sendCommandViaHTTP(
                            deviceHTTPEndpoint,
                            VirtualFireAlarmConstants.TEMPERATURE_CONTEXT,
                            false);

                    SensorDataManager.getInstance().setSensorRecord(deviceId,
                                                                    VirtualFireAlarmConstants.SENSOR_TEMP,
                                                                    temperatureValue,
                                                                    Calendar.getInstance().getTimeInMillis());
                    break;

                case MQTT_PROTOCOL:
                    String mqttResource = VirtualFireAlarmConstants.TEMPERATURE_CONTEXT.replace("/", "");
                    virtualFireAlarmMQTTConnector.publishDeviceData(owner, deviceId, mqttResource, "");
                    break;

                case XMPP_PROTOCOL:
                    String xmppResource = VirtualFireAlarmConstants.TEMPERATURE_CONTEXT.replace("/", "");
                    virtualFireAlarmMQTTConnector.publishDeviceData(owner, deviceId, xmppResource, "");
                    break;

                default:
                    response.setStatus(Response.Status.NOT_ACCEPTABLE.getStatusCode());
            }
            sensorRecord = SensorDataManager.getInstance().getSensorRecord(deviceId,
                                                                           VirtualFireAlarmConstants.SENSOR_TEMP);
        } catch (DeviceManagementException | DeviceControllerException | TransportHandlerException e) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

        response.setStatus(Response.Status.OK.getStatusCode());
        return sensorRecord;
    }

    /**
     * @param dataMsg
     * @param response
     */
    @Path("controller/push_temperature")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public void pushTemperatureData(final DeviceJSON dataMsg,
                                    @Context HttpServletResponse response) {
        String deviceId = dataMsg.deviceId;
        String deviceIp = dataMsg.reply;
        float temperature = dataMsg.value;

        String registeredIp = deviceToIpMap.get(deviceId);

        if (registeredIp == null) {
            log.warn("Unregistered IP: Temperature Data Received from an un-registered IP " +
                             deviceIp + " for device ID - " + deviceId);
            response.setStatus(Response.Status.PRECONDITION_FAILED.getStatusCode());
            return;
        } else if (!registeredIp.equals(deviceIp)) {
            log.warn("Conflicting IP: Received IP is " + deviceIp + ". Device with ID " + deviceId +
                             " is already registered under some other IP. Re-registration required");
            response.setStatus(Response.Status.CONFLICT.getStatusCode());
            return;
        }
        SensorDataManager.getInstance().setSensorRecord(deviceId, VirtualFireAlarmConstants.SENSOR_TEMP,
                                                        String.valueOf(temperature),
                                                        Calendar.getInstance().getTimeInMillis());

        if (!VirtualFireAlarmServiceUtils.publishToDAS(dataMsg.owner, dataMsg.deviceId, dataMsg.value)) {
            response.setStatus(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
        }

    }


    /**
     * @param operation
     * @param message
     * @return
     */
    @GET
    @Path("controller/scep")
    public Response scepRequest(@QueryParam("operation") String operation, @QueryParam("message") String message) {

        if (log.isDebugEnabled()) {
            log.debug("Invoking SCEP operation " + operation);
        }

        if (SCEPOperation.GET_CA_CERT.getValue().equals(operation)) {

            if (log.isDebugEnabled()) {
                log.debug("Invoking GetCACert");
            }

            try {
                CertificateManagementService certificateManagementService =
                        VirtualFireAlarmServiceUtils.getCertificateManagementService();
                SCEPResponse scepResponse = certificateManagementService.getCACertSCEP();
                Response.ResponseBuilder responseBuilder;

                switch (scepResponse.getResultCriteria()) {
                    case CA_CERT_FAILED:
                        log.error("CA cert failed");
                        responseBuilder = Response.serverError();
                        break;
                    case CA_CERT_RECEIVED:

                        if (log.isDebugEnabled()) {
                            log.debug("CA certificate received in GetCACert");
                        }
                        responseBuilder = Response.ok(scepResponse.getEncodedResponse(), ContentType.X_X509_CA_CERT);
                        break;
                    case CA_RA_CERT_RECEIVED:

                        if (log.isDebugEnabled()) {
                            log.debug("CA and RA certificates received in GetCACert");
                        }

                        responseBuilder = Response.ok(scepResponse.getEncodedResponse(), ContentType.X_X509_CA_RA_CERT);
                        break;
                    default:
                        log.error("Invalid SCEP request");
                        responseBuilder = Response.serverError();
                        break;
                }

                return responseBuilder.build();
            } catch (VirtualFireAlarmException e) {
                log.error("Error occurred while enrolling the VirtualFireAlarm device", e);
            } catch (KeystoreException e) {
                log.error("Keystore error occurred while enrolling the VirtualFireAlarm device", e);
            }

        } else if (SCEPOperation.GET_CA_CAPS.getValue().equals(operation)) {

            if (log.isDebugEnabled()) {
                log.debug("Invoking GetCACaps");
            }

            try {
                CertificateManagementService certificateManagementService = VirtualFireAlarmServiceUtils.
                        getCertificateManagementService();
                byte caCaps[] = certificateManagementService.getCACapsSCEP();

                return Response.ok(caCaps, MediaType.TEXT_PLAIN).build();

            } catch (VirtualFireAlarmException e) {
                log.error("Error occurred while enrolling the device", e);
            }

        } else {
            log.error("Invalid SCEP operation " + operation);
        }

        return Response.serverError().build();
    }

    /**
     * @param operation
     * @param inputStream
     * @return
     */
    @POST
    @Path("controller/scep")
    public Response scepRequestPost(@QueryParam("operation") String operation, InputStream inputStream) {

        if (log.isDebugEnabled()) {
            log.debug("Invoking SCEP operation " + operation);
        }

        if (SCEPOperation.PKI_OPERATION.getValue().equals(operation)) {

            if (log.isDebugEnabled()) {
                log.debug("Invoking PKIOperation");
            }

            try {
                CertificateManagementService certificateManagementService = VirtualFireAlarmServiceUtils.
                        getCertificateManagementService();
                byte pkiMessage[] = certificateManagementService.getPKIMessageSCEP(inputStream);

                return Response.ok(pkiMessage, ContentType.X_PKI_MESSAGE).build();

            } catch (VirtualFireAlarmException e) {
                log.error("Error occurred while enrolling the device", e);
            } catch (KeystoreException e) {
                log.error("Keystore error occurred while enrolling the device", e);
            }
        }
        return Response.serverError().build();
    }
}