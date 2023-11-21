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

const stateLib = require('@adobe/aio-lib-state');
const crypto = require('crypto');
const { getAioLogger, COPY_ACTION, DELETE_ACTION } = require('./utils');
const appConfig = require('./appConfig');

const FG_KEY = 'FLOODGATE';

/**
 * This is helper to track status of the Floodgate process.
 * The project status is stored data in to libstate and can be retrieved.
 */
class FgStatus {
    /**
     * Status constants
     */
    static PROJECT_STATUS = {
        STARTED: 'STARTED',
        NOT_STARTED: 'NOT STARTED',
        COMPLETED: 'COMPLETED',
        COMPLETED_WITH_ERROR: 'COMPLETED WITH ERROR',
        IN_PROGRESS: 'IN PROGRESS',
        FAILED: 'FAILED'
    };

    /**
     * Status constants
     */
    static PROJECT_STAGE = {
        PROMOTE_COPY_STATUS: 'promoteCopyStatus',
    };

    /**
     * Template that will be populated and stored in state
     */
    storeStatusTmpl = {
        action: {
            lastTriggeredBy: '',
            type: '',
            status: '',
            message: '',
            activationId: '',
            startTime: '',
            endTime: '',
            batchesInfo: [],
            details: {}
        }
    };

    storeStatus = JSON.parse(JSON.stringify(this.storeStatusTmpl));

    /**
     * Constructor with initial setup
     * @param {*} options statusKey is the key to be used to store
     * and actionType is the type of FG Action like copy or promote
     */
    constructor({
        action,
        statusKey,
        keySuffix,
        userDetails
    }) {
        this.lastTriggeredBy = userDetails?.oid;
        this.action = action || '';
        this.storeKey = statusKey || this.generateStoreKey(keySuffix) || FG_KEY;
        this.logger = getAioLogger();
    }

    generateStoreKey(keySuffix) {
        const siteFgRootPath = appConfig.getSiteFgRootPath();
        const { projectExcelPath } = appConfig.getPayload();
        let resp = '';

        switch (this.action) {
            case COPY_ACTION:
                resp = `${siteFgRootPath || ''}${projectExcelPath || ''}`;
                break;
            case DELETE_ACTION:
                resp = `${DELETE_ACTION}${siteFgRootPath}`;
                break;
            default:
                resp = siteFgRootPath;
        }
        resp += keySuffix || '';
        return resp;
    }

    getStoreKey() {
        return this.storeKey;
    }

    /**
     * Checks if the given status is to be treated as InProgress
     * @param {*} status Status to be checked
     * @returns true, if the FG Status InProgress or Started
     */
    static isInProgress(status) {
        return [FgStatus.PROJECT_STATUS.IN_PROGRESS,
            FgStatus.PROJECT_STATUS.STARTED].includes(status);
    }

    /**
     * Checks if the given status is to be treated as completed
     * @param {*} status Status to be checked
     * @returns true, if the FG Status Completed (or with Error) or Failed
     */
    static isFinished(status) {
        return [FgStatus.PROJECT_STATUS.COMPLETED, FgStatus.PROJECT_STATUS.COMPLETED_WITH_ERROR,
            FgStatus.PROJECT_STATUS.FAILED].includes(status);
    }

    /**
     * Updates the status in storeStatus and stores to libstate
     * @param {*} optinos, status, message are required to be passed in most cases other optional
     * fields are activationId, startTime, endTime, action
     * @returns progress object which is stored in the store
     */
    async updateStatusToStateLib({
        status, statusMessage, activationId, action, startTime, endTime, batches, details
    }) {
        try {
            await this.getStatusFromStateLib().then(async (result) => {
                if (result?.action) {
                    this.storeStatus = result;
                    if (status) {
                        this.storeStatus.action.status = status;
                    }
                    if (statusMessage) {
                        this.storeStatus.action.message = statusMessage;
                    }
                    if (activationId) {
                        this.storeStatus.action.activationId = activationId;
                    }
                    if (startTime) {
                        this.storeStatus.action.startTime = startTime;
                    }
                    if (endTime) {
                        this.storeStatus.action.endTime = endTime;
                    }
                } else {
                    this.storeStatus.action.status = status;
                    this.storeStatus.action.message = statusMessage;
                    this.storeStatus.action.activationId = activationId;
                    this.storeStatus.action.startTime = startTime || this.storeStatus.action.startTime;
                    this.storeStatus.action.endTime = endTime || this.storeStatus.action.endTime;
                }
                if (details) {
                    this.storeStatus.action.details = { ...this.storeStatus.action.details, ...details };
                }
                this.storeStatus.action.batches = batches || this.storeStatus.action.batches;
                this.storeStatus.action.type = action || this.action || this.storeStatus.action.type;
                this.storeStatus.action.lastTriggeredBy = this.lastTriggeredBy ||
                    this.storeStatus.action.lastTriggeredBy;

                // Set start and end based on status
                if (status && FgStatus.PROJECT_STATUS.STARTED === status) {
                    this.storeStatus.action.startTime = startTime || new Date();
                }
                if (FgStatus.isFinished(this.storeStatus.action.status)) {
                    this.storeStatus.action.endTime = endTime || new Date();
                    if (!this.storeStatus.action.startTime) {
                        this.storeStatus.action.startTime = this.storeStatus.action.endTime;
                    }
                }
                await this.updateStateStatus();
            });
        } catch (err) {
            this.logger.error(`Error creating state store ${err}`);
        }
        return this.storeStatus;
    }

    // Get the start end data from state
    getStartEndTime() {
        const { startTime, endTime } = this.storeStatus.action;
        return { startTime, endTime };
    }

    /**
     * Get the status object from libstate
     * @returns Status object
     */
    async getStatusFromStateLib() {
        let status;
        try {
            // md5 hash of the config file
            const hash = crypto.createHash('md5').update(this.storeKey).digest('hex');
            this.logger.debug(`Project path and hash value -- ${this.storeKey} and ${hash}`);
            // init when running in an Adobe I/O Runtime action (OpenWhisk) (uses env vars __OW_API_KEY and __OW_NAMESPACE automatically)
            const state = await stateLib.init();
            // getting activation id data from io state
            const res = await state.get(hash); // res = { value, expiration }
            if (res) {
                status = res.value;
                this.logger.debug(`Status from the store ${JSON.stringify(status)}`);
            }
        } catch (err) {
            this.logger.error(`Error getting data from state store ${err}`);
        }
        return status;
    }

    /**
     * Save the store status object into libstate
     */
    async updateStateStatus() {
        const hash = this.getHash();
        this.logger.info(`Adding status to aio state lib with hash ${this.storeKey} -- ${hash} - ${JSON.stringify(this.storeStatus)}`);
        // get the hash value if its available
        try {
            const state = await stateLib.init();
            // save it
            await state.put(hash, this.storeStatus, {
                // 30day expiration...
                ttl: 2592000
            });
        } catch (err) {
            this.logger.error(`Error creating state store ${err}`);
        }
    }

    getHash() {
        return crypto.createHash('md5').update(this.storeKey).digest('hex');
    }

    async clearState(remove) {
        if (remove) {
            const hash = this.getHash();
            try {
                const state = await stateLib.init();
                await state.delete(hash);
            } catch (err) {
                this.logger.error(`Error deleting from state store ${err}`);
            }
        } else {
            await this.updateStateStatus(this.storeStatusTmpl);
        }
    }
}

module.exports = FgStatus;
