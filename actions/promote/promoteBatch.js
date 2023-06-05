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
const { PROJECT_STATUS } = require('../project');
const urlInfo = require('../urlInfo');
const {
    getAuthorizedRequestOption, saveFile, updateExcelTable, getFileUsingDownloadUrl, fetchWithRetry
} = require('../sharepoint');
const {
    getAioLogger, simulatePreviewPublish, handleExtension, updateStatusToStateLib, PROMOTE_ACTION, delay, PREVIEW, PUBLISH, logMemUsage
} = require('../utils');
const appConfig = require('../appConfig');

const BATCH_REQUEST_PROMOTE = 50;
const MAX_CHILDREN = 1000;

async function main(params) {
    const ow = openwhisk();
    const logger = getAioLogger();
    logMemUsage();
    let payload;
    const {
        adminPageUri, projectExcelPath, fgRootFolder, doPublish,
    } = params;
    appConfig.setAppConfig(params);

    try {
        if (!fgRootFolder) {
            payload = 'Required data is not available to proceed with FG Promote action.';
            logger.error(payload);
        } else if (!adminPageUri || !projectExcelPath) {
            payload = 'Required data is not available to proceed with FG Promote action.';
            updateStatusToStateLib(fgRootFolder, PROJECT_STATUS.FAILED, payload, undefined, PROMOTE_ACTION);
            logger.error(payload);
        } else {
            urlInfo.setUrlInfo(adminPageUri);
            payload = 'Getting all files to be promoted.';
            updateStatusToStateLib(fgRootFolder, PROJECT_STATUS.IN_PROGRESS, payload, undefined, PROMOTE_ACTION);
            logger.info(payload);
            payload = await promoteInBatch(ow, {adminPageUri, projectExcelPath, fgRootFolder, doPublish,}, adminPageUri, projectExcelPath, doPublish);
            updateStatusToStateLib(fgRootFolder, PROJECT_STATUS.COMPLETED, payload, undefined, PROMOTE_ACTION);
        }
    } catch (err) {
        updateStatusToStateLib(fgRootFolder, PROJECT_STATUS.COMPLETED_WITH_ERROR, err.message, undefined, PROMOTE_ACTION);
        logger.error(err);
        payload = err;
    }

    return {
        body: payload,
    };
}

/**
 * Find all files in the pink tree to promote.
 */
async function findAllFiles(adminPageUri) {
    const logger = getAioLogger();
    logger.info(`ADMIN PAGE URI: ${adminPageUri}`);
    const { sp } = await getConfig(adminPageUri);
    logger.info(`SP TOKEN: ${sp}`);
    const baseURI = `${sp.api.excel.update.fgBaseURI}`;
    const rootFolder = baseURI.split('/').pop();
    const options = await getAuthorizedRequestOption({ method: 'GET' });

    return findAllFloodgatedFiles(baseURI, options, rootFolder, [], ['']);
}

/**
 * Iteratively finds all files under a specified root folder.
 */
async function findAllFloodgatedFiles(baseURI, options, rootFolder, fgFiles, fgFolders) {
    while (fgFolders.length !== 0) {
        const uri = `${baseURI}${fgFolders.shift()}:/children?$top=${MAX_CHILDREN}`;
        // eslint-disable-next-line no-await-in-loop
        const res = await fetchWithRetry(uri, options);
        if (res.ok) {
            // eslint-disable-next-line no-await-in-loop
            const json = await res.json();
            const driveItems = json.value;
            driveItems?.forEach((item) => {
                const itemPath = `${item.parentReference.path.replace(`/drive/root:/${rootFolder}`, '')}/${item.name}`;
                if (item.folder) {
                    // it is a folder
                    fgFolders.push(itemPath);
                } else {
                    const downloadUrl = item['@microsoft.graph.downloadUrl'];
                    fgFiles.push({ fileDownloadUrl: downloadUrl, filePath: itemPath });
                }
            });
        }
    }

    return fgFiles;
}

async function promoteInBatch(ow, params, adminPageUri, projectExcelPath, doPublish) {
    const logger = getAioLogger();
    const startPromote = new Date();
    let payload = 'Getting all floodgated files to promote...';
    // Iterate the floodgate tree and get all files to promote
    const allFloodgatedFiles = await findAllFiles(adminPageUri);
    // create batches to process the data
    const batchArray = [];
    for (let i = 0; i < allFloodgatedFiles.length; i += BATCH_REQUEST_PROMOTE) {
        const arrayChunk = allFloodgatedFiles.slice(i, i + BATCH_REQUEST_PROMOTE);
        // eslint-disable-next-line no-await-in-loop
        batchArray.push(await triggerActivation(ow, projectExcelPath, {...params, files: arrayChunk}));
    }
    payload = {
        code: 200,
        payload: batchArray,
    }
    return payload;
}

async function triggerActivation(ow, projectPath, args) {
    const logger = getAioLogger();
    logger.info('triggerActivation :');
    const str = JSON.stringify(args);
    logger.info(str);
    return ow.actions.invoke({
        name: 'milo-fg/promote-worker',
        blocking: false, // this is the flag that instructs to execute the worker asynchronous
        result: false,
        params: args
    }).then(async (result) => {
        logger.info("RESULT");
        logger.info(result);
        //  attaching activation id to the status
        const payload = await updateStatusToStateLib(projectPath, PROJECT_STATUS.IN_PROGRESS, undefined, result.activationId, PROMOTE_ACTION);
        return {
            code: 200,
            payload,
        };
    }).catch(async (err) => {
        const payload = await updateStatusToStateLib(projectPath, PROJECT_STATUS.FAILED, `Failed to invoke actions ${err.message}`, undefined, PROMOTE_ACTION);
        logger.error('Failed to invoke actions', err);
        return {
            code: 500,
            payload
        };
    });
}

exports.main = main;