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
const {
    errorResponse, getAioLogger, getInstanceKey, PROMOTE_ACTION
} = require('../utils');
const appConfig = require('../appConfig');
const FgUser = require('../fgUser');
const BatchManager = require('../batchManager');

const logger = getAioLogger();
const BAD_REQUEST_SC = 400;
const AUTH_FAILED_SC = 401;
const GEN_ERROR_SC = 500;

/**
 * Returns promote status. The details of the status needed are passed a arguments.
 * Sample Input
 *  {
 *      "promoteStatus": true,
 *      "batchFiles": 2,
 *      "batchResults": 2,
 *      "promoteResults": true,
 *      "fgShareUrl": "https://adobe.sharepoint.com/:f:/r/sites/adobecom/Shared%20Documents/milo-pink<relativePath>?web=1",
 *      "spToken": ""
 *  }
 * @param {*} args Action arguments
 * @returns results based on parameter below
 *  "promoteStatus": { batchesInfo: [ {"activationId": "", "batchNumber": 1, "done": true,
 *      "endTime": "2023-09-19T13:53:32.846Z", "startTime": "2023-09-07T10:00:00.333Z", "status": "COMPLETED"}, ] }
 *  "batchFiles": [
 *      "/drafts/fold/lvl1/lvl2/testforfloodgate2.docx",
 *      "/drafts/fold/lvl11/lvl2/testforfloodgate.docx"
 *  ]
 * "batchResults": {
 *      "failedPreviews": [
 *          "/drafts/fold/lvl11/testforfloodgate2",
 *          "/drafts/fold/lvl1/lvl2/testforfloodgate"
 *      ],
 *      "failedPromotes": [],
 *      "failedPublishes": []
 * }
 * "promoteResults": {
 *      "failedPreviews": [
 *          "/drafts/fold/lvl11/testforfloodgate2",
 *          "/drafts/fold/lvl1/lvl2/testforfloodgate",
 *          "/drafts/fold/lvl1/lvl2/testforfloodgate2",
 *          "/drafts/fold/lvl11/lvl2/testforfloodgate"
 *      ],
 *      "failedPromotes": [],
 *      "failedPublishes": []
 * }
 */
async function main(args) {
    const payload = {};
    try {
        appConfig.setAppConfig(args);
        const batchNumber = args.batchFiles || args.batchResults;

        // Validations
        const fgUser = new FgUser({ at: args.spToken });
        if (!args.fgShareUrl) {
            return exitAction(errorResponse(BAD_REQUEST_SC, 'Mising required fgShareUrl parameter'));
        }

        if (!await fgUser.isUser()) {
            return exitAction(errorResponse(AUTH_FAILED_SC, 'Authentication failed. Please refresh page and try again.'));
        }

        // Starts
        const siteFgRootPath = appConfig.getSiteFgRootPath();
        const batchManager = new BatchManager({ key: PROMOTE_ACTION, instanceKey: getInstanceKey({ fgRootFolder: siteFgRootPath }) });
        await batchManager.init({ batchNumber });
        const currentBatch = batchNumber ? await batchManager.getCurrentBatch() : null;

        // Read instance_info.json
        if (args.promoteStatus !== undefined) {
            const instanceContent = await batchManager.getInstanceFileContent();
            if (!instanceContent || !instanceContent.dtls) {
                throw new Error('Missing instance content!');
            }
            payload.promoteStatus = { batchesInfo: instanceContent?.dtls?.batchesInfo };
        }

        if (args.batchFiles !== undefined) {
            const batchFilesContent = await currentBatch.getFiles();
            payload.batchFiles = batchFilesContent?.map((e) => e.file?.filePath);
        }

        if (args.batchResults !== undefined) {
            const brC = await currentBatch.getResultsContent();
            if (brC) payload.batchResults = brC;
        }

        if (args.promoteResults !== undefined) {
            const prC = await batchManager.getResultsContent();
            if (prC) payload.promoteResults = prC;
        }
    } catch (err) {
        logger.error(err);
        return exitAction(errorResponse(GEN_ERROR_SC, `Something went wrong: ${err}`));
    }

    return exitAction({
        ...payload
    });
}

function exitAction(resp) {
    appConfig.removePayload();
    return resp;
}

exports.main = main;
