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
const { updateExcelTable } = require('../sharepoint');
const {
    getAioLogger, PROMOTE_ACTION, PROMOTE_BATCH, actInProgress
} = require('../utils');
const appConfig = require('../appConfig');
const FgStatus = require('../fgStatus');
const BatchManager = require('../batchManager');
const FgAction = require('../FgAction');

const logger = getAioLogger();

async function main(params) {
    let respPayload;

    const valParams = {
        statParams: ['fgRootFolder'],
        actParams: ['adminPageUri', 'projectExcelPath'],
        checkUser: false,
        checkStatus: false,
        checkActivation: false
    };
    const ow = openwhisk();

    appConfig.setAppConfig(params);
    const batchManager = new BatchManager({ key: PROMOTE_ACTION });
    await batchManager.init();
    // Read instance_info.json
    const instanceContent = await batchManager.getInstanceData();
    if (!instanceContent || !instanceContent.dtls) {
        return exitAction({ body: 'None to run!' });
    }

    const { batchesInfo } = instanceContent.dtls;

    // Initialize action
    const fgAction = new FgAction(PROMOTE_ACTION, { ...params, ...instanceContent.dtls });
    fgAction.init({ ow, skipUserDetails: true });
    const { fgStatus } = fgAction.getActionParams();
    const { payload } = appConfig.getConfig();

    try {
        const vStat = await fgAction.validateAction(valParams);
        if (vStat && vStat.code !== 200) {
            return exitAction(vStat);
        }

        const promoteProg = batchesInfo.reduce((acc, item) => {
            acc.total += 1;
            acc.prog += item.done || item.activationId ? 1 : 0;
            return acc;
        }, { total: 0, prog: 0 });

        respPayload = promoteProg.prog ? `Promoting batch ${promoteProg.prog} / ${promoteProg.total}.` : 'Promoting files.';
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.IN_PROGRESS,
            statusMessage: respPayload
        });

        // Check to see all batches are complete
        const { anyInProg, allCopyDone } = await checkBatchesInProg(payload.fgRootFolder, batchesInfo, ow);
        await batchManager.writeToInstanceFile(instanceContent);

        // Collect status and mark as complete
        if (allCopyDone) {
            const allDone = await checkPostPromoteStatus(payload.fgRootFolder, batchesInfo);
            if (allDone) {
                await completePromote(payload.projectExcelPath, batchesInfo, batchManager, fgStatus);
            }
            await batchManager.writeToInstanceFile(instanceContent);
        } else if (!anyInProg) {
            // Trigger next activation
            const nextItem = batchesInfo.find((b) => !b.activationId);
            const batchNumber = nextItem?.batchNumber;
            if (batchNumber) {
                const newActDtls = await triggerPromoteWorkerAction(ow,
                    {
                        ...appConfig.getPassthruParams(),
                        batchNumber
                    },
                    fgStatus);
                nextItem.activationId = newActDtls?.activationId;
            }
            await batchManager.writeToInstanceFile(instanceContent);
        }

        respPayload = 'Promote trigger and track completed.';
        logger.info(respPayload);
    } catch (err) {
        logger.error(err);
        respPayload = err;
        // In case of error log status with end time
        try {
            await fgStatus.updateStatusToStateLib({
                status: FgStatus.PROJECT_STATUS.COMPLETED_WITH_ERROR,
                statusMessage: err.message,
            });
        } catch (err2) {
            logger.info('Error while updatnig failed status');
        }
    }
    return exitAction({
        body: respPayload,
    });
}

/**
 * Checks if activation is in progress by inspecting state and activations
 * @param {*} fgRootFolder Root folder
 * @param {*} actDtls activation details like activation id
 * @param {*} ow Openwisk api interface
 * @returns flag if any activation is in progress
 */
async function checkBatchesInProg(fgRootFolder, actDtls, ow) {
    let fgStatus;
    let batchState;
    let batchInProg = false;
    let allCopyDone = true;
    let counter = 0;
    for (; counter < actDtls?.length && !batchInProg; counter += 1) {
        const {
            batchNumber,
            activationId,
            copyDone
        } = actDtls[counter];
        if (activationId && !copyDone) {
            fgStatus = new FgStatus({
                action: `${PROMOTE_BATCH}_${batchNumber}`,
                keySuffix: `Batch_${batchNumber}`
            });
            batchState = await fgStatus.getStatusFromStateLib().then((result) => result?.action);
            // Track and trigger called before the state is marked in progress with activation id
            batchInProg = false || (!batchState?.status && !batchState?.actInProgress) || FgStatus.isInProgress(batchState?.details?.[FgStatus.PROJECT_STAGE.PROMOTE_COPY_STATUS]);
            if (batchInProg) batchInProg = await actInProgress(ow, activationId, batchInProg);
            if (!batchInProg) {
                actDtls[counter].copyDone = true;
                actDtls[counter].startTime = batchState?.startTime;
                actDtls[counter].status = batchState?.status;
            } else if (batchState) {
                actDtls[counter].startTime = batchState.startTime;
                actDtls[counter].status = batchState.status;
            }
            allCopyDone &&= !batchInProg;
        } else {
            allCopyDone &&= copyDone;
        }
    }

    return { anyInProg: batchInProg, allCopyDone };
}

async function checkPostPromoteStatus(fgRootFolder, actDtls) {
    let fgStatus;
    let batchState;
    let batchInProg = false;
    let allDone = true;
    let counter = 0;
    for (; counter < actDtls?.length && !batchInProg; counter += 1) {
        const {
            batchNumber,
            activationId,
            copyDone,
            done
        } = actDtls[counter];
        if (activationId && copyDone && !done) {
            fgStatus = new FgStatus({
                action: `${PROMOTE_BATCH}_${batchNumber}`,
                keySuffix: `Batch_${batchNumber}`
            });
            batchState = await fgStatus.getStatusFromStateLib().then((result) => result?.action);
            // Track and trigger called before the state is marked in progress with activation id
            batchInProg = false || (!batchState?.status && !batchState?.actInProgress) || FgStatus.isInProgress(batchState?.status);
            if (!batchInProg) {
                actDtls[counter].done = true;
                actDtls[counter].startTime = batchState?.startTime;
                actDtls[counter].endTime = batchState?.endTime;
                actDtls[counter].status = batchState?.status;
            }
            allDone &&= !batchInProg;
        } else {
            allDone &&= done;
        }
    }
    return allDone;
}

async function triggerPromoteWorkerAction(ow, params, fgStatus) {
    return ow.actions.invoke({
        name: 'milo-fg/promote-worker',
        blocking: false, // this is the flag that instructs to execute the worker asynchronous
        result: false,
        params
    }).then(async (result) => {
        // attaching activation id to the status
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.IN_PROGRESS,
            activationId: result.activationId,
            details: {
                [FgStatus.PROJECT_STAGE.PROMOTE_COPY_STATUS]: FgStatus.PROJECT_STATUS.STARTED
            }
        });
        return {
            batchNumber: params.batchNumber,
            activationId: result.activationId
        };
    }).catch(async (err) => {
        await fgStatus.updateStatusToStateLib({
            status: FgStatus.PROJECT_STATUS.FAILED,
            statusMessage: `Failed to invoke actions ${err.message} for batch ${params.batchNumber}`
        });
        logger.error(`Failed for batch ${params.batchNumber}`, err);
        return {
            batchNumber: params.batchNumber
        };
    });
}

/**
 * Marks the proocess as complete and collects all errors and updates excel.
 * @param {*} projectExcelPath Project excel where status needs to be updated
 * @param {*} actDtls activation details like id
 * @param {*} batchManager BatchManager to get batch details like path
 * @param {*} fgStatus Floodgate status instance to update state
 */
async function completePromote(projectExcelPath, actDtls, batchManager, fgStatus) {
    let batchNumber;
    let results;
    const failedPromotes = [];
    const failedPreviews = [];
    const failedPublishes = [];
    for (let i = 0; i < actDtls?.length || 0; i += 1) {
        batchNumber = actDtls[i].batchNumber;
        try {
            batchManager.initBatch({ batchNumber });
            const batch = await batchManager.getCurrentBatch();
            results = await batch.getResultsContent();
            if (results?.failedPromotes?.length > 0) {
                failedPromotes.push(...results.failedPromotes);
            }
            if (results?.failedPreviews?.length > 0) {
                failedPreviews.push(...results.failedPreviews);
            }
            if (results?.failedPublishes?.length > 0) {
                failedPublishes.push(...results.failedPublishes);
            }
        } catch (err) {
            logger.error(`Error while reading batch content in tracker ${err}`);
        }
    }

    const fgErrors = failedPromotes.length > 0 || failedPreviews.length > 0 ||
        failedPublishes.length > 0;

    // Write to Excel
    await fgStatus.updateStatusToStateLib({
        status: fgErrors ? FgStatus.PROJECT_STATUS.COMPLETED_WITH_ERROR : FgStatus.PROJECT_STATUS.COMPLETED,
        statusMessage: fgErrors ?
            'Error occurred when promoting floodgated content. Check project excel sheet for additional information.' :
            'Promoted floodgate tree successfully.'
    });

    const { startTime: startPromote, endTime: endPromote } = fgStatus.getStartEndTime();
    const excelValues = [['PROMOTE', startPromote, endPromote, failedPromotes.join('\n'), failedPreviews.join('\n'), failedPublishes.join('\n')]];
    await updateExcelTable(projectExcelPath, 'PROMOTE_STATUS', excelValues);
    logger.info('Project excel file updated with promote status.');

    await batchManager.markComplete(fgErrors ? { failedPromotes, failedPreviews, failedPublishes } : null);
    logger.info('Marked complete in batch manager.');
}

function exitAction(resp) {
    appConfig.removePayload();
    return resp;
}
exports.main = main;
