/* ************************************************************************
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

// eslint-disable-next-line import/no-extraneous-dependencies
const openwhisk = require('openwhisk');
const {
    getAioLogger, PROMOTE_ACTION
} = require('../utils');
const FgStatus = require('../fgStatus');
const FgAction = require('../FgAction');
const appConfig = require('../appConfig');

// This returns the activation ID of the action that it called
async function main(args) {
    const logger = getAioLogger();
    let respPayload;
    const valParams = {
        statParams: ['fgRootFolder'],
        actParams: ['adminPageUri', 'projectExcelPath'],
        checkUser: true,
        checkStatus: true,
        checkActivation: false
    };
    const ow = openwhisk();
    // Initialize action
    const fgAction = new FgAction(PROMOTE_ACTION, args);
    fgAction.init({ ow });
    const { fgStatus } = fgAction.getActionParams();

    try {
        // Validations
        const vStat = await fgAction.validateAction(valParams);
        if (vStat && vStat.code !== 200) {
            return exitAction(vStat);
        }
        fgAction.logStart();

        await fgStatus.clearState();
        respPayload = await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.STARTED,
            statusMessage: 'Triggering promote action',
            batches: {}
        });
        logger.info(`FGStatus store ${await fgStatus.getStatusFromStateLib()}`);

        return exitAction(ow.actions.invoke({
            name: 'milo-fg/promote-create-batch',
            blocking: false, // this is the flag that instructs to execute the worker asynchronous
            result: false,
            params: appConfig.getPassthruParams()
        }).then(async (result) => {
            //  attaching activation id to the status
            respPayload = await fgStatus.updateStatusToStateLib({
                status: FgStatus.PROJECT_STATUS.IN_PROGRESS,
                activationId: result.activationId
            });
            return {
                code: 200,
                payload: respPayload
            };
        }).catch(async (err) => {
            respPayload = await fgStatus.updateStatusToStateLib({
                status: FgStatus.PROJECT_STATUS.FAILED,
                statusMessage: `Failed to invoke actions ${err.message}`
            });
            return {
                code: 500,
                payload: respPayload
            };
        }));
    } catch (err) {
        logger.error(err);
        respPayload = await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.FAILED,
            statusMessage: `Failed to invoke actions ${err.message}`
        });
    }

    return exitAction({
        code: 500,
        payload: respPayload,
    });
}

function exitAction(resp) {
    appConfig.removePayload();
    return resp;
}

exports.main = main;
