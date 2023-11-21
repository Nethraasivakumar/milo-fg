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
const { getConfig } = require('../config');
const {
    getAuthorizedRequestOption, saveFile, getFileUsingDownloadUrl, fetchWithRetry
} = require('../sharepoint');
const {
    getAioLogger, delay, logMemUsage, getInstanceKey, PROMOTE_ACTION, PROMOTE_BATCH
} = require('../utils');
const FgAction = require('../FgAction');
const FgStatus = require('../fgStatus');
const BatchManager = require('../batchManager');
const appConfig = require('../appConfig');

const DELAY_TIME_PROMOTE = 3000;

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
    logger.info(`Promote started for ${batchNumber}`);
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

        await fgStatus.clearState();

        respPayload = 'Getting all files to be promoted.';
        logger.info(respPayload);
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.STARTED,
            statusMessage: respPayload,
            details: {
                [FgStatus.PROJECT_STAGE.PROMOTE_COPY_STATUS]: FgStatus.PROJECT_STATUS.IN_PROGRESS
            }

        });
        respPayload = 'Promote files';
        logger.info(respPayload);
        respPayload = await promoteFloodgatedFiles(payload.doPublish, batchManager);
        respPayload = `Promoted files ${JSON.stringify(respPayload)}`;
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.IN_PROGRESS,
            statusMessage: respPayload,
            details: {
                [FgStatus.PROJECT_STAGE.PROMOTE_COPY_STATUS]: FgStatus.PROJECT_STATUS.COMPLETED
            }
        });
        // A small delay before trigger
        await delay(DELAY_TIME_PROMOTE);
        await triggerPostCopy(ow, { ...appConfig.getPassthruParams(), batchNumber }, fgStatus);
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

/**
 * Copies the Floodgated files back to the main content tree.
 * Creates intermediate folders if needed.
 */
async function promoteCopy(srcPath, destinationFolder) {
    const { sp } = await getConfig();
    const { baseURI } = sp.api.file.copy;
    const rootFolder = baseURI.split('/').pop();
    const payload = { ...sp.api.file.copy.payload, parentReference: { path: `${rootFolder}${destinationFolder}` } };
    const options = await getAuthorizedRequestOption({
        method: sp.api.file.copy.method,
        body: JSON.stringify(payload),
    });

    // copy source is the pink directory for promote
    const copyStatusInfo = await fetchWithRetry(`${sp.api.file.copy.fgBaseURI}${srcPath}:/copy?@microsoft.graph.conflictBehavior=replace`, options);
    const statusUrl = copyStatusInfo.headers.get('Location');
    let copySuccess = false;
    let copyStatusJson = {};
    while (statusUrl && !copySuccess && copyStatusJson.status !== 'failed') {
        // eslint-disable-next-line no-await-in-loop
        const status = await fetchWithRetry(statusUrl);
        if (status.ok) {
            // eslint-disable-next-line no-await-in-loop
            copyStatusJson = await status.json();
            copySuccess = copyStatusJson.status === 'completed';
        }
    }
    return copySuccess;
}

async function promoteFloodgatedFiles(doPublish, batchManager) {
    const logger = getAioLogger();

    async function promoteFile(batchItem) {
        const { fileDownloadUrl, filePath } = batchItem.file;
        const status = { success: false, srcPath: filePath };
        try {
            let promoteSuccess = false;
            const destinationFolder = `${filePath.substring(0, filePath.lastIndexOf('/'))}`;
            const copyFileStatus = await promoteCopy(filePath, destinationFolder);
            if (copyFileStatus) {
                promoteSuccess = true;
            } else {
                const file = await getFileUsingDownloadUrl(fileDownloadUrl);
                const saveStatus = await saveFile(file, filePath);
                if (saveStatus.success) {
                    promoteSuccess = true;
                }
            }
            status.success = promoteSuccess;
        } catch (error) {
            const errorMessage = `Error promoting files ${fileDownloadUrl} at ${filePath} to main content tree ${error.message}`;
            logger.error(errorMessage);
            status.success = false;
        }
        return status;
    }

    let i = 0;
    let stepMsg = 'Getting all floodgated files to promote.';
    // Get the batch files using the batchmanager for the assigned batch and process them
    const currentBatch = await batchManager.getCurrentBatch();
    const currBatchLbl = `Batch-${currentBatch.getBatchNumber()}`;
    const allFloodgatedFiles = await currentBatch?.getFiles();
    logger.info(`Files for the batch are ${allFloodgatedFiles.length}`);
    // create batches to process the data
    const batchArray = [];
    const numBulkReq = appConfig.getNumBulkReq();
    for (i = 0; i < allFloodgatedFiles.length; i += numBulkReq) {
        const arrayChunk = allFloodgatedFiles.slice(i, i + numBulkReq);
        batchArray.push(arrayChunk);
    }

    // process data in batches
    const promoteStatuses = [];
    for (i = 0; i < batchArray.length; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        promoteStatuses.push(...await Promise.all(
            batchArray[i].map((bi) => promoteFile(bi))
        ));
        // eslint-disable-next-line no-await-in-loop, no-promise-executor-return
        await delay(DELAY_TIME_PROMOTE);
    }

    stepMsg = `Completed promoting all documents in the batch ${currBatchLbl}`;
    logger.info(stepMsg);

    const failedPromotes = promoteStatuses.filter((status) => !status.success)
        .map((status) => status.srcPath || 'Path Info Not available');
    logger.info(`Promote ${currBatchLbl}, Prm: ${failedPromotes?.length}`);

    if (failedPromotes.length > 0) {
        stepMsg = 'Error occurred when promoting floodgated content. Check project excel sheet for additional information.';
        logger.info(stepMsg);
        // Write the information to batch manifest
        await currentBatch.writeResults({ failedPromotes });
    } else {
        stepMsg = `Promoted floodgate for ${currBatchLbl} successfully`;
        logger.info(stepMsg);
    }
    logMemUsage();
    stepMsg = `Floodgate promote (copy) of ${currBatchLbl} is completed`;
    return stepMsg;
}

async function triggerPostCopy(ow, params, fgStatus) {
    return ow.actions.invoke({
        name: 'milo-fg/post-copy-worker',
        blocking: false, // this is the flag that instructs to execute the worker asynchronous
        result: false,
        params
    }).then(async (result) => {
        // attaching activation id to the status
        // Its possible status is updated in post copy action before this callback is called.
        await fgStatus.updateStatusToStateLib({
            postPromoteActivationId: result.activationId
        });
        return {
            postPromoteActivationId: result.activationId
        };
    }).catch(async (err) => {
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.FAILED,
            statusMessage: `Failed to invoke actions ${err.message}`
        });
        getAioLogger().error(`Failed to invoke actions for batch ${params.batchNumber}`, err);
        return {};
    });
}

function exitAction(resp) {
    appConfig.removePayload();
    return resp;
}

exports.main = main;
