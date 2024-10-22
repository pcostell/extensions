/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import config from "./config";
import * as functions from "firebase-functions";
import {
  onDocumentWritten,
  FirestoreEvent,
} from "firebase-functions/v2/firestore";
const { onTaskDispatched } = require("firebase-functions/v2/tasks");
import * as admin from "firebase-admin";
import { getExtensions } from "firebase-admin/extensions";
import { getFunctions } from "firebase-admin/functions";
import { getFirestore } from "firebase-admin/firestore";

import {
  ChangeType,
  FirestoreBigQueryEventHistoryTracker,
  FirestoreEventHistoryTracker,
} from "@firebaseextensions/firestore-bigquery-change-tracker";

import { getEventarc } from "firebase-admin/eventarc";
import * as logs from "./logs";
import * as events from "./events";
import { getChangeType, getDocumentId, resolveWildcardIds } from "./util";

// Hard coded settings because the config variables are only accessible at function
// runtime and these are being used at deploy time.
const PROJECT_ID = "";
const DATABASE_ID = "";
// Should match config.location;
const DB_LOCATION = "";
// Location is either the location of the database if regional or if the database is
// multiregional use us-central1 for nam5 and europe-west3 for eur3.
const LOCATION = DB_LOCATION;
const DOCUMENT_PATH = "";

const eventTrackerConfig = {
  tableId: config.tableId,
  datasetId: config.datasetId,
  datasetLocation: config.datasetLocation,
  backupTableId: config.backupCollectionId,
  transformFunction: config.transformFunction,
  timePartitioning: config.timePartitioning,
  timePartitioningField: config.timePartitioningField,
  timePartitioningFieldType: config.timePartitioningFieldType,
  timePartitioningFirestoreField: config.timePartitioningFirestoreField,
  databaseId: config.databaseId,
  clustering: config.clustering,
  wildcardIds: config.wildcardIds,
  bqProjectId: config.bqProjectId,
  useNewSnapshotQuerySyntax: config.useNewSnapshotQuerySyntax,
  // Set to false in order to trigger BQ resource creation. After that, set to
  // true and redeploy to avoid unnecessary calls to BigQuery.
  skipInit: false,
  kmsKeyName: config.kmsKeyName,
};

const eventTracker: FirestoreEventHistoryTracker =
  new FirestoreBigQueryEventHistoryTracker(eventTrackerConfig);

logs.init();

/** Init app, if not already initialized */
if (admin.apps.length === 0) {
  admin.initializeApp();
}

events.setupEventChannel();

export const syncbigquery = onTaskDispatched(
  { region: LOCATION, retry: true },
  async (request) => {
    /** Record the chnages in the change tracker */
    await eventTracker.record([{ ...request.data }]);

    /** Send an event Arc update , if configured */
    await events.recordSuccessEvent({
      subject: request.data.documentId,
      data: {
        ...request.data,
      },
    });

    logs.complete();
  }
);

export const fsexportbigquery = onDocumentWritten(
  {
    document: DOCUMENT_PATH,
    database: DATABASE_ID,
    region: DB_LOCATION,
    retry: true,
  },
  async (event) => {
    const change = event.data;
    logs.start();
    const changeType = getChangeType(change);
    const documentId = getDocumentId(change);

    const isCreated = changeType === ChangeType.CREATE;
    const isDeleted = changeType === ChangeType.DELETE;

    const data = isDeleted ? undefined : change.after?.data();
    const oldData =
      isCreated || config.excludeOldData ? undefined : change.before?.data();

    /**
     * Serialize early before queueing in cloud task
     * Cloud tasks currently have a limit of 1mb, this also ensures payloads are kept to a minimum
     */
    let serializedData: any;
    let serializedOldData: any;

    try {
      serializedData = eventTracker.serializeData(data);
      serializedOldData = eventTracker.serializeData(oldData);
    } catch (err) {
      logs.error(false, "Failed to serialize data", err, null, null);
      throw err;
    }

    try {
      await events.recordStartEvent({
        documentId,
        changeType,
        before: { data: change.before.data() },
        after: { data: change.after.data() },
      });
    } catch (err) {
      logs.error(false, "Failed to record start event", err, null, null);
      throw err;
    }

    const eventData = {
      timestamp: event.time, // This is a Cloud Firestore commit timestamp with microsecond precision.
      operation: changeType,
      documentName: event.document,
      documentId: documentId,
      pathParams: config.wildcardIds ? event.params : null,
      eventId: event.id,
      data: serializedData,
      oldData: serializedOldData,
    };

    try {
      const queue = getFunctions().taskQueue(
        `locations/${config.location}/functions/syncbigquery`
      );

      await queue.enqueue(eventData);
    } catch (err) {
      await events.recordErrorEvent(err as Error);
      // Only log the error once here
      if (!err.logged) {
        logs.error(
          config.logFailedExportData,
          "Failed to enqueue task to syncBigQuery",
          err,
          eventData,
          eventTrackerConfig
        );
      }
      return;
    }

    logs.complete();
  }
);
