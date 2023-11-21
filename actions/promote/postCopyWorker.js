/* eslint-disable no-await-in-loop */
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
const openwhisk = require('openwhisk');
const {
    handleExtension, getAioLogger, logMemUsage, getInstanceKey, PROMOTE_ACTION, PROMOTE_BATCH
} = require('../utils');
const helixUtils = require('../helixUtils');
const FgAction = require('../FgAction');
const FgStatus = require('../fgStatus');
const BatchManager = require('../batchManager');
const appConfig = require('../appConfig');

async function main(params) {
    const logger = getAioLogger();
    logMemUsage();
    const { batchNumber } = params;
    const valParams = {
        statParams: ['fgRootFolder', 'projectExcelPath'],
        actParams: ['adminPageUri'],
        checkUser: false,
        checkStatus: false,
        checkActivation: false
    };
    const ow = openwhisk();
    // Initialize action
    logger.info(`Post promote worker started for batch ${batchNumber}`);
    const fgAction = new FgAction(`${PROMOTE_BATCH}_${batchNumber}`, params);
    fgAction.init({ ow, skipUserDetails: true, fgStatusParams: { keySuffix: `Batch_${batchNumber}` } });
    const { fgStatus } = fgAction.getActionParams();
    const payload = appConfig.getPayload();
    const fgRootFolder = appConfig.getSiteFgRootPath();

    let respPayload;
    const batchManager = new BatchManager({ key: PROMOTE_ACTION, instanceKey: getInstanceKey({ fgRootFolder }) });
    await batchManager.init({ batchNumber });
    try {
        const vStat = await fgAction.validateAction(valParams);
        if (vStat && vStat.code !== 200) {
            return exitAction(vStat);
        }

        respPayload = 'Previewing/Publishing promoted content';
        logger.info(respPayload);
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.IN_PROGRESS,
            statusMessage: respPayload
        });

        respPayload = await previewPublish(payload.doPublish, batchManager);
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.COMPLETED,
            statusMessage: respPayload
        });
    } catch (err) {
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.COMPLETED_WITH_ERROR,
            statusMessage: err.message,
        });
        logger.error(err);
        respPayload = err;
    }

    return exitAction({
        body: respPayload,
    });
}

async function previewPublish(doPublish, batchManager) {
    const logger = getAioLogger();

    let stepMsg = 'Getting all batch files.';
    // Get the batch files using the batchmanager for the assigned batch and process them
    const currentBatch = await batchManager.getCurrentBatch();
    const currBatchLbl = `Batch-${currentBatch.getBatchNumber()}`;
    const allFloodgatedFiles = await currentBatch.getFiles();
    const promotedFiles = allFloodgatedFiles.map((e) => e.file.filePath);
    const resultsContent = await currentBatch.getResultsContent() || {};
    const failedPromotes = resultsContent.failedPromotes || [];
    const prevPaths = promotedFiles.filter((item) => !failedPromotes.includes(item)).map((e) => handleExtension(e));
    logger.info(`Post promote files for ${currBatchLbl} are ${prevPaths?.length}`);

    logger.info('Previewing promoted files.');
    let previewStatuses = [];
    let publishStatuses = [];
    if (helixUtils.canBulkPreviewPublish()) {
        previewStatuses = await helixUtils.bulkPreviewPublish(prevPaths, helixUtils.getOperations().PREVIEW);
        stepMsg = 'Completed generating Preview for promoted files.';
        logger.info(stepMsg);

        if (doPublish) {
            stepMsg = 'Publishing promoted files.';
            logger.info(stepMsg);
            publishStatuses = await helixUtils.bulkPreviewPublish(prevPaths, helixUtils.getOperations().LIVE);
            stepMsg = 'Completed Publishing for promoted files';
            logger.info(stepMsg);
        }
    }

    const failedPreviews = previewStatuses.filter((status) => !status.success)
        .map((status) => status.path);
    const failedPublishes = publishStatuses.filter((status) => !status.success)
        .map((status) => status.path);
    logger.info(`Post promote ${currBatchLbl}, Prm: ${failedPromotes?.length}, Prv: ${failedPreviews?.length}, Pub: ${failedPublishes?.length}`);

    if (failedPromotes.length > 0 || failedPreviews.length > 0 || failedPublishes.length > 0) {
        stepMsg = 'Error occurred when promoting floodgated content. Check project excel sheet for additional information.';
        logger.info(stepMsg);
        // Write the information to batch manifest
        currentBatch.writeResults({ failedPromotes, failedPreviews, failedPublishes });
        throw new Error(stepMsg);
    }
    logMemUsage();
    logger.info(`All tasks for promote ${currBatchLbl} is completed`);
    stepMsg = 'All tasks for floodgate promote is completed';
    return stepMsg;
}

function exitAction(resp) {
    appConfig.removePayload();
    return resp;
}

exports.main = main;
