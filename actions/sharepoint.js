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

const { Headers } = require('node-fetch');
const fetch = require('node-fetch');
const { getConfig } = require('./config');
const { getAioLogger } = require('./utils');
const appConfig = require('./appConfig');
const sharepointAuth = require('./sharepointAuth');

const SP_CONN_ERR_LST = ['ETIMEDOUT', 'ECONNRESET'];
const APP_USER_AGENT = 'ISV|Adobe|MiloFloodgate/0.1.0';
const BATCH_REQUEST_LIMIT = 20;
const BATCH_DELAY_TIME = 200;
const NUM_REQ_THRESHOLD = 5;
const RETRY_ON_CF = 3;
const TOO_MANY_REQUESTS = '429';
// Added for debugging rate limit headers
const LOG_RESP_HEADER = false;
let nextCallAfter = 0;
const itemIdMap = {};

// eslint-disable-next-line default-param-last
async function getAuthorizedRequestOption({ body = null, json = true, method = 'GET' } = {}) {
    const appSpToken = await sharepointAuth.getAccessToken();
    const bearer = `Bearer ${appSpToken}`;

    const headers = new Headers();
    headers.append('Authorization', bearer);
    headers.append('User-Agent', APP_USER_AGENT);
    if (json) {
        headers.append('Accept', 'application/json');
        headers.append('Content-Type', 'application/json');
    }

    const options = {
        method,
        headers,
    };

    if (body) {
        options.body = typeof body === 'string' ? body : JSON.stringify(body);
    }

    return options;
}

async function executeGQL(url, opts) {
    const options = await getAuthorizedRequestOption(opts);
    const res = await fetchWithRetry(url, options);
    if (!res.ok) {
        throw new Error(`Failed to execute ${url}`);
    }
    return res.json();
}

async function getItemId(uri, path) {
    const key = `~${uri}~${path}~`;
    itemIdMap[key] = itemIdMap[key] || await executeGQL(`${uri}${path}?$select=id`);
    return itemIdMap[key]?.id;
}

async function getDriveRoot(accessToken) {
    const logger = getAioLogger();
    try {
        const headers = new Headers();
        headers.append('Authorization', `Bearer ${accessToken}`);
        headers.append('User-Agent', APP_USER_AGENT);
        headers.append('Accept', 'application/json');
        const fgSite = appConfig.getFgSite();
        const response = await fetchWithRetry(`${fgSite}/drive/root`, { headers });

        if (response?.ok) {
            const driveDtls = await response.json();
            return driveDtls;
        }
        logger.info(`Unable to get User details: ${response?.status}`);
    } catch (error) {
        logger.info('Unable to fetch User Info');
        logger.info(JSON.stringify(error));
    }
    return null;
}

async function getFileData(filePath, isFloodgate) {
    const { sp } = await getConfig();
    const options = await getAuthorizedRequestOption();
    const baseURI = isFloodgate ? sp.api.directory.create.fgBaseURI : sp.api.directory.create.baseURI;
    const resp = await fetchWithRetry(`${baseURI}${filePath}`, options);
    const json = await resp.json();
    const fileDownloadUrl = json['@microsoft.graph.downloadUrl'];
    const fileSize = json.size;
    return { fileDownloadUrl, fileSize };
}

async function getFilesData(filePaths, isFloodgate) {
    const batchArray = [];
    for (let i = 0; i < filePaths.length; i += BATCH_REQUEST_LIMIT) {
        const arrayChunk = filePaths.slice(i, i + BATCH_REQUEST_LIMIT);
        batchArray.push(arrayChunk);
    }
    // process data in batches
    const fileJsonResp = [];
    for (let i = 0; i < batchArray.length; i += 1) {
        // eslint-disable-next-line no-await-in-loop
        fileJsonResp.push(...await Promise.all(
            batchArray[i].map((file) => getFileData(file, isFloodgate)),
        ));
        // eslint-disable-next-line no-await-in-loop, no-promise-executor-return
        await new Promise((resolve) => setTimeout(resolve, BATCH_DELAY_TIME));
    }
    return fileJsonResp;
}

async function getFile(doc) {
    if (doc && doc.sp && doc.sp.status === 200) {
        const response = await fetchWithRetry(doc.sp.fileDownloadUrl);
        return response.blob();
    }
    return undefined;
}

async function getFileUsingDownloadUrl(downloadUrl) {
    const options = await getAuthorizedRequestOption({ json: false });
    const response = await fetchWithRetry(downloadUrl, options);
    if (response) {
        return response.blob();
    }
    return undefined;
}

async function createFolder(folder, isFloodgate) {
    const { sp } = await getConfig();
    const options = await getAuthorizedRequestOption({ method: sp.api.directory.create.method });
    options.body = JSON.stringify(sp.api.directory.create.payload);

    const baseURI = isFloodgate ? sp.api.directory.create.fgBaseURI : sp.api.directory.create.baseURI;
    const res = await fetchWithRetry(`${baseURI}${folder}`, options);
    if (res.ok) {
        return res.json();
    }
    throw new Error(`Could not create folder: ${folder}`);
}

function getFolderFromPath(path) {
    if (path.includes('.')) {
        return path.substring(0, path.lastIndexOf('/'));
    }
    return path;
}

function getFileNameFromPath(path) {
    return path.split('/').pop().split('/').pop();
}

async function createUploadSession(sp, file, dest, filename, isFloodgate) {
    const payload = {
        ...sp.api.file.createUploadSession.payload,
        description: 'Preview file',
        fileSize: file.size,
        name: filename,
    };
    const options = await getAuthorizedRequestOption({ method: sp.api.file.createUploadSession.method });
    options.body = JSON.stringify(payload);

    const baseURI = isFloodgate ? sp.api.file.createUploadSession.fgBaseURI : sp.api.file.createUploadSession.baseURI;

    const createdUploadSession = await fetchWithRetry(`${baseURI}${dest}:/createUploadSession`, options);
    return createdUploadSession.ok ? createdUploadSession.json() : undefined;
}

async function uploadFile(sp, uploadUrl, file) {
    const options = await getAuthorizedRequestOption({
        json: false,
        method: sp.api.file.upload.method,
    });
    // TODO API is limited to 60Mb, for more, we need to batch the upload.
    options.headers.append('Content-Length', file.size);
    options.headers.append('Content-Range', `bytes 0-${file.size - 1}/${file.size}`);
    options.headers.append('Prefer', 'bypass-shared-lock');
    options.body = file;
    return fetchWithRetry(`${uploadUrl}`, options);
}

async function deleteFile(sp, filePath) {
    const options = await getAuthorizedRequestOption({
        json: false,
        method: sp.api.file.delete.method,
    });
    options.headers.append('Prefer', 'bypass-shared-lock');
    return fetch(filePath, options);
}

async function renameFile(spFileUrl, filename) {
    const options = await getAuthorizedRequestOption({ method: 'PATCH', body: JSON.stringify({ name: filename }) });
    options.headers.append('Prefer', 'bypass-shared-lock');
    return fetch(spFileUrl, options);
}

async function releaseUploadSession(sp, uploadUrl) {
    await deleteFile(sp, uploadUrl);
}

function getLockedFileNewName(filename) {
    const extIndex = filename.indexOf('.');
    const fileNameWithoutExtn = filename.substring(0, extIndex);
    const fileExtn = filename.substring(extIndex);
    return `${fileNameWithoutExtn}-locked-${Date.now()}${fileExtn}`;
}

async function createSessionAndUploadFile(sp, file, dest, filename, isFloodgate) {
    const createdUploadSession = await createUploadSession(sp, file, dest, filename, isFloodgate);
    const status = {};
    if (createdUploadSession) {
        const uploadSessionUrl = createdUploadSession.uploadUrl;
        if (!uploadSessionUrl) {
            return status;
        }
        status.sessionUrl = uploadSessionUrl;
        const uploadedFile = await uploadFile(sp, uploadSessionUrl, file);
        if (!uploadedFile) {
            return status;
        }
        if (uploadedFile.ok) {
            status.uploadedFile = await uploadedFile.json();
            status.success = true;
        } else if (uploadedFile.status === 423) {
            status.locked = true;
        }
    }
    return status;
}

/**
 * The method gets the list of files, extracts the parent path, extracts uniq paths,
 * filters common parents urls
 * e.g.. [/a/b/one.txt, /a/b/two.txt, /a/c/three.txt, /a/c/d/three.txt]
 * Folders to create would be [/a/b, /a/c/d]
 * This triggers async and waits for batch to complete. These are small batches so should be fast.
 * The $batch can be used in future to submit only one URL
 * @param {*} srcPathList Paths of files for which folder creating is needed
 * @param {*} isFloodgate Is floodgate flag
 * @returns Create folder status
 */
async function bulkCreateFolders(srcPathList, isFloodgate) {
    const logger = getAioLogger();
    const createtFolderStatuses = [];
    const allPaths = srcPathList.map((e) => {
        if (e.length < 2 || !e[1]?.doc) return '';
        return getFolderFromPath(e[1].doc.filePath);
    }).filter((e) => true && e);
    const uniqPathLst = Array.from(new Set(allPaths));
    const leafPathLst = uniqPathLst.filter((e) => uniqPathLst.findIndex((e1) => e1.indexOf(`${e}/`) >= 0) < 0);
    // logger.info(`Unique path list ${JSON.stringify(leafPathLst)}`);
    try {
        logger.info('bulkCreateFolders started');
        const promises = leafPathLst.map((folder) => createFolder(folder, isFloodgate));
        logger.info('Got createfolder promises and waiting....');
        createtFolderStatuses.push(...await Promise.all(promises));
        logger.info(`bulkCreateFolders completed ${createtFolderStatuses?.length}`);
        // logger.info(`bulkCreateFolders statuses ${JSON.stringify(createtFolderStatuses)}`);
    } catch (error) {
        logger.info('Error while creating folders');
        logger.info(error?.stack);
    }
    logger.info(`bulkCreateFolders returning ${createtFolderStatuses?.length}`);
    return createtFolderStatuses;
}

async function copyFile(srcPath, destinationFolder, newName, isFloodgate, isFloodgateLockedFile) {
    const logger = getAioLogger();
    const { sp } = await getConfig();
    const { baseURI, fgBaseURI } = sp.api.file.copy;
    const rootFolder = isFloodgate ? fgBaseURI.split('/').pop() : baseURI.split('/').pop();

    const payload = { ...sp.api.file.copy.payload, parentReference: { path: `${rootFolder}${destinationFolder}` } };
    if (newName) {
        payload.name = newName;
    }
    const options = await getAuthorizedRequestOption({
        method: sp.api.file.copy.method,
        body: JSON.stringify(payload),
    });
    // In case of FG copy action triggered via saveFile(), locked file copy happens in the floodgate content location
    // So baseURI is updated to reflect the destination accordingly
    const contentURI = isFloodgate && isFloodgateLockedFile ? fgBaseURI : baseURI;
    const copyStatusInfo = await fetchWithRetry(`${contentURI}${srcPath}:/copy?@microsoft.graph.conflictBehavior=replace`, options);
    const statusUrl = copyStatusInfo.headers.get('Location');
    let copySuccess = false;
    let copyStatusJson = {};
    if (!statusUrl) {
        logger.info(`Copy of ${srcPath} returned ${copyStatusInfo?.status} with no followup URL`);
    }
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

async function saveFile(file, dest, isFloodgate) {
    try {
        const folder = getFolderFromPath(dest);
        const filename = getFileNameFromPath(dest);
        await createFolder(folder, isFloodgate);
        const { sp } = await getConfig();
        let uploadFileStatus = await createSessionAndUploadFile(sp, file, dest, filename, isFloodgate);
        if (uploadFileStatus.locked) {
            await releaseUploadSession(sp, uploadFileStatus.sessionUrl);
            const lockedFileNewName = getLockedFileNewName(filename);
            const baseURI = isFloodgate ? sp.api.file.get.fgBaseURI : sp.api.file.get.baseURI;
            const spFileUrl = `${baseURI}${dest}`;
            await renameFile(spFileUrl, lockedFileNewName);
            const newLockedFilePath = `${folder}/${lockedFileNewName}`;
            const copyFileStatus = await copyFile(newLockedFilePath, folder, filename, isFloodgate, true);
            if (copyFileStatus) {
                uploadFileStatus = await createSessionAndUploadFile(sp, file, dest, filename, isFloodgate);
                if (uploadFileStatus.success) {
                    await deleteFile(sp, `${baseURI}${newLockedFilePath}`);
                }
            }
        }
        const uploadedFileJson = uploadFileStatus.uploadedFile;
        if (uploadedFileJson) {
            return { success: true, uploadedFileJson, path: dest };
        }
    } catch (error) {
        return { success: false, path: dest, errorMsg: error.message };
    }
    return { success: false, path: dest };
}

async function getExcelTable(excelPath, tableName) {
    const { sp } = await getConfig();
    const itemId = await getItemId(sp.api.file.get.baseURI, excelPath);
    if (itemId) {
        const tableJson = await executeGQL(`${sp.api.excel.get.baseItemsURI}/${itemId}/workbook/tables/${tableName}/rows`);
        return !tableJson?.value ? [] :
            tableJson.value
                .filter((e) => e.values?.find((rw) => rw.find((col) => col)))
                .map((e) => e.values);
    }
    return [];
}

async function deleteFloodgateDir() {
    const logger = getAioLogger();
    logger.info('Deleting content started.');
    const { sp } = await getConfig();
    let deleteSuccess = false;

    const { fgDirPattern } = appConfig.getConfig();
    const fgRegExp = new RegExp(fgDirPattern);
    logger.info(fgRegExp);
    if (fgRegExp.test(sp.api.file.update.fgBaseURI)) {
        const temp = '/temp';
        const finalBaserURI = `${sp.api.file.delete.fgBaseURI}${temp}`;
        logger.info(`Deleting the folder ${finalBaserURI} `);
        try {
            await deleteFile(sp, finalBaserURI);
            deleteSuccess = true;
        } catch (error) {
            logger.info(`Error occurred when trying to delete files of main content tree ${error.message}`);
        }
    }
    return deleteSuccess;
}

async function updateExcelTable(excelPath, tableName, values) {
    const { sp } = await getConfig();
    const itemId = await getItemId(sp.api.file.get.baseURI, excelPath);
    if (itemId) {
        return executeGQL(`${sp.api.excel.update.baseItemsURI}/${itemId}/workbook/tables/${tableName}/rows`, {
            body: JSON.stringify({ values }),
            method: sp.api.excel.update.method,
        });
    }
    return {};
}

// fetch-with-retry added to check for Sharepoint RateLimit headers and 429 errors and to handle them accordingly.
async function fetchWithRetry(apiUrl, options, retryCounts) {
    let retryCount = retryCounts || 0;
    const logger = getAioLogger();
    return new Promise((resolve, reject) => {
        const currentTime = Date.now();
        if (retryCount > NUM_REQ_THRESHOLD) {
            reject();
        } else if (nextCallAfter !== 0 && currentTime < nextCallAfter) {
            setTimeout(() => fetchWithRetry(apiUrl, options, retryCount)
                .then((newResp) => resolve(newResp))
                .catch((err) => reject(err)), nextCallAfter - currentTime);
        } else {
            retryCount += 1;
            fetch(apiUrl, options).then((resp) => {
                logHeaders(resp);
                const retryAfter = resp.headers.get('ratelimit-reset') || resp.headers.get('retry-after') || 0;
                if ((resp.headers.get('test-retry-status') === TOO_MANY_REQUESTS) || (resp.status === TOO_MANY_REQUESTS)) {
                    nextCallAfter = Date.now() + retryAfter * 1000;
                    logger.info(`Retry ${nextCallAfter}`);
                    fetchWithRetry(apiUrl, options, retryCount)
                        .then((newResp) => resolve(newResp))
                        .catch((err) => reject(err));
                } else {
                    nextCallAfter = retryAfter ? Math.max(Date.now() + retryAfter * 1000, nextCallAfter) : nextCallAfter;
                    resolve(resp);
                }
            }).catch((err) => {
                logger.warn(`Connection error ${apiUrl} with ${JSON.stringify(err)}`);
                if (err && SP_CONN_ERR_LST.includes(err.code) && retryCount < NUM_REQ_THRESHOLD) {
                    logger.info(`Retry ${SP_CONN_ERR_LST}`);
                    nextCallAfter = Date.now() + RETRY_ON_CF * 1000;
                    return fetchWithRetry(apiUrl, options, retryCount)
                        .then((newResp) => resolve(newResp))
                        .catch((err2) => reject(err2));
                }
                return reject(err);
            });
        }
    });
}

function getHeadersStr(response) {
    const headers = {};
    response?.headers?.forEach((value, name) => {
        headers[name] = value;
    });
    return JSON.stringify(headers);
}

function logHeaders(response) {
    if (!LOG_RESP_HEADER) return;
    const logger = getAioLogger();
    const hdrStr = getHeadersStr(response);
    const logStr = `Status is ${response.status} with headers ${hdrStr}`;

    if (logStr.toUpperCase().indexOf('RATE') > 0 || logStr.toUpperCase().indexOf('RETRY') > 0) logger.info(logStr);
}

module.exports = {
    getAuthorizedRequestOption,
    executeGQL,
    getDriveRoot,
    getExcelTable,
    getFilesData,
    getFile,
    getFileUsingDownloadUrl,
    copyFile,
    saveFile,
    createFolder,
    updateExcelTable,
    fetchWithRetry,
    getFolderFromPath,
    getFileNameFromPath,
    bulkCreateFolders,
    deleteFloodgateDir,
};
