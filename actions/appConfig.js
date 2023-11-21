/* ***********************************************************************
 * ADOBE CONFIDENTIAL
 * ___________________
 *
 * Copyright 2023 Adobe
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains
 * the property of Adobe and its suppliers, if any. The intellectual
 * and technical concepts contained herein are proprietary to Adobe
 * and its suppliers and are protected by all applicable intellectual
 * property laws, including trade secret and copyright laws.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Adobe.
 ************************************************************************* */

const crypto = require('crypto');
const { getAioLogger } = require('./utils');
const UrlInfo = require('./urlInfo');

// Max activation is 1hrs, set to 2hrs
const MAX_ACTIVATION_TIME = 2 * 60 * 60 * 1000;
const ENV_VAR_ACTIVATION_ID = '__OW_ACTIVATION_ID';

/**
 * This store the Floodate configs.
 * Common Configs - Parameters like Batch
 */
class AppConfig {
    // set payload per activation
    configMap = { payload: {} };

    setAppConfig(params) {
        const payload = this.initPayload();
        // Called during action start to cleanup old entries
        this.removeOldPayload();

        // These are payload parameters
        payload.spToken = params.spToken;
        payload.adminPageUri = params.adminPageUri;
        payload.projectExcelPath = params.projectExcelPath;
        payload.shareUrl = params.shareUrl;
        payload.fgShareUrl = params.fgShareUrl;
        payload.rootFolder = params.rootFolder;
        payload.fgRootFolder = params.fgRootFolder;
        payload.promoteIgnorePaths = params.promoteIgnorePaths || ['/.milo', '/.helix', '/metadata.xlsx'];
        payload.doPublish = params.doPublish;
        payload.driveId = params.driveId;
        payload.fgColor = params.fgColor || 'pink';

        // These are from configs and not activation related
        this.configMap.fgSite = params.fgSite;
        this.configMap.fgClientId = params.fgClientId;
        this.configMap.fgAuthority = params.fgAuthority;
        this.configMap.clientId = params.clientId;
        this.configMap.tenantId = params.tenantId;
        this.configMap.certPassword = params.certPassword;
        this.configMap.certKey = params.certKey;
        this.configMap.certThumbprint = params.certThumbprint;
        this.configMap.skipInProg = (params.skipInProgressCheck || '').toLowerCase() === 'true';
        this.configMap.batchFilesPath = params.batchFilesPath || 'milo-floodgate/batching';
        this.configMap.maxFilesPerBatch = parseInt(params.maxFilesPerBatch || '200', 10);
        this.configMap.numBulkReq = parseInt(params.numBulkReq || '20', 10);
        this.configMap.groupCheckUrl = params.groupCheckUrl || 'https://graph.microsoft.com/v1.0/groups/{groupOid}/members?$count=true';
        this.configMap.fgUserGroups = this.getJsonFromStr(params.fgUserGroups, []);
        this.configMap.fgAdminGroups = this.getJsonFromStr(params.fgAdminGroups, []);
        this.configMap.fgDirPattern = params.fgDirPattern || '-(pink|blue|purple)$';
        this.configMap.siteRootPathRex = this.siteRootPathRex || '.*/sites(/.*)<';
        this.configMap.helixAdminApiKeys = this.getJsonFromStr(params.helixAdminApiKeys);
        this.configMap.bulkPreviewCheckInterval = parseInt(params.bulkPreviewCheckInterval || '30', 10);
        this.configMap.maxBulkPreviewChecks = parseInt(params.maxBulkPreviewChecks || '30', 10);
        this.extractPrivateKey();

        payload.ext = {
            siteRootPath: this.extractSiteRootPath(params.shareUrl),
            siteFgRootPath: this.extractSiteRootPath(params.fgShareUrl),
            urlInfo: payload.adminPageUri ? new UrlInfo(payload.adminPageUri) : null
        };
    }

    // Activation Payload Related
    initPayload() {
        this.configMap.payload[this.getPayloadKey()] = {
            payloadAccessedOn: new Date().getTime()
        };
        return this.configMap.payload[this.getPayloadKey()];
    }

    getPayloadKey() {
        return process.env[ENV_VAR_ACTIVATION_ID];
    }

    getPayload() {
        this.configMap.payload[this.getPayloadKey()].payloadAccessedOn = new Date().getTime();
        return this.configMap.payload[this.getPayloadKey()];
    }

    removePayload() {
        delete this.configMap.payload[this.getPayloadKey()];
    }

    /**
     * Similar to LRU
     */
    removeOldPayload() {
        const { payload } = this.configMap;
        const payloadKeys = Object.keys(payload);
        const leastTime = new Date().getTime();
        payloadKeys
            .filter((key) => payload[key]?.payloadAccessedOn < leastTime - MAX_ACTIVATION_TIME)
            .forEach((key) => delete payload[key]);
    }

    // Configs related methods
    getConfig() {
        const { payload, ...configMap } = this.configMap;
        return { ...configMap, payload: this.getPayload() };
    }

    getJsonFromStr(str, def = {}) {
        try {
            return JSON.parse(str);
        } catch (err) {
            // Mostly bad string ignored
            getAioLogger().debug(`Error while parsing ${str}`);
        }
        return def;
    }

    /**
     * Parameter that was part of payload.
     * Avoid access tokens, No PASS or SECRET Keys to be passed
     * @returns key-value
     */
    getPassthruParams() {
        const {
            spToken,
            ext,
            payloadAccessedOn,
            ...payloadParams
        } = this.getPayload();
        return payloadParams;
    }

    getMsalConfig() {
        const {
            clientId, tenantId, certPassword, pvtKey, certThumbprint,
        } = this.configMap;
        return {
            clientId, tenantId, certPassword, pvtKey, certThumbprint,
        };
    }

    getFgSite() {
        return this.configMap.fgSite;
    }

    getPromoteIgnorePaths() {
        return this.getPayload().promoteIgnorePaths;
    }

    extractPrivateKey() {
        if (!this.configMap.certKey) return;
        const decodedKey = Buffer.from(
            this.configMap.certKey,
            'base64'
        ).toString('utf-8');
        this.configMap.pvtKey = crypto
            .createPrivateKey({
                key: decodedKey,
                passphrase: this.configMap.certPassword,
                format: 'pem',
            })
            .export({
                format: 'pem',
                type: 'pkcs8',
            });
    }

    getSkipInProgressCheck() {
        return true && this.configMap.skipInProg;
    }

    getBatchConfig() {
        return {
            batchFilesPath: this.configMap.batchFilesPath,
            maxFilesPerBatch: this.configMap.maxFilesPerBatch,
        };
    }

    getNumBulkReq() {
        return this.configMap.numBulkReq;
    }

    extractSiteRootPath(shareUrl) {
        try {
            return shareUrl.match(new RegExp(this.configMap.siteRootPathRex))[1];
        } catch (err) {
            return '/';
        }
    }

    getSiteFgRootPath() {
        return this.getPayload().ext.siteFgRootPath;
    }

    getUrlInfo() {
        return this.getPayload().ext.urlInfo;
    }
}

module.exports = new AppConfig();
