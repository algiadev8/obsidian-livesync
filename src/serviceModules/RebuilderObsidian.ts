import { REMOTE_P2P } from "@vrtmrz/livesync-commonlib/compat/common/models/setting.const";
import type { Rebuilder } from "@vrtmrz/livesync-commonlib/compat/interfaces/DatabaseRebuilder";
import { ServiceRebuilder, type ServiceRebuilderDependencies } from "@vrtmrz/livesync-commonlib/compat/serviceModules/Rebuilder";
import { DEFAULT_SETTINGS } from "@vrtmrz/livesync-commonlib/settings";
import { LOG_LEVEL_NOTICE, LOG_LEVEL_VERBOSE } from "octagonal-wheels/common/logger";
import { delay } from "octagonal-wheels/promises";

export class ServiceRebuilderObsidian extends ServiceRebuilder implements Rebuilder {
    constructor(private readonly rebuildServices: ServiceRebuilderDependencies) {
        super(rebuildServices);
    }

    override async $rebuildEverything(): Promise<void> {
        await this.rebuildEverything();
    }

    override async rebuildEverything(): Promise<void> {
        await this.rebuildServices.replicator.runBoundedRemoteActivity(() => this.performObsidianRebuildEverything(), {
            label: "rebuild-everything",
        });
        await this.informOptionalFeatures();
    }

    private async performObsidianRebuildEverything(): Promise<void> {
        const services = this.rebuildServices;
        services.appLifecycle.resetIsReady();
        await services.setting.suspendExtraSync();
        await services.setting.applyPartial({
            isConfigured: true,
            notifyThresholdOfRemoteStorageSize: DEFAULT_SETTINGS.notifyThresholdOfRemoteStorageSize,
        });
        await services.control.applySettings();
        await this.resetLocalDatabase();
        await delay(1000);
        await this.prepareLocalDatabaseFromStorage();
        if (services.setting.currentSettings().remoteType === REMOTE_P2P) {
            if (!(await this.completePreparedObsidianRebuild())) {
                throw new Error("The local P2P rebuild could not be finalised.");
            }
            return;
        }
        await services.replication.markLocked();
        await this.tryResetRemoteDatabase();
        await services.replication.markLocked();
        await delay(500);
        await delay(1000);
        if (!(await services.replication.replicateAllToRemoteForRebuild(true))) {
            throw new Error("The first rebuild upload did not complete.");
        }
        await delay(1000);
        if (!(await services.replication.replicateAllToRemoteForRebuild(true))) {
            throw new Error("The final rebuild upload did not complete.");
        }
        if (!(await this.completePreparedObsidianRebuild())) {
            throw new Error("The rebuild could not be finalised.");
        }
    }

    private async prepareLocalDatabaseFromStorage(): Promise<void> {
        const services = this.rebuildServices;
        if (!services.database.isDatabaseReady()) {
            throw new Error("The selected local database is not ready for rebuild preparation.");
        }
        const files = await services.storageAccess.getFiles();
        let processed = 0;
        let failed = 0;
        for (const file of files) {
            if (services.vault.isFileSizeTooLarge(file.stat.size)) {
                this._log(`REBUILD STORAGE -> DB : ${file.path} has been skipped due to file size exceeding the limit`, LOG_LEVEL_NOTICE);
                continue;
            }
            if (!(await services.fileHandler.storeFileToDB(file, true))) {
                failed++;
                this._log(`REBUILD STORAGE -> DB : ${file.path} failed`, LOG_LEVEL_NOTICE);
            }
            processed++;
            if (processed % 25 === 0) {
                this._log(`Processing: ${processed}/${files.length}`, LOG_LEVEL_NOTICE, "syncAll");
            }
        }
        if (failed !== 0) {
            throw new Error(`The Vault could not be scanned for rebuild preparation. ${failed} file(s) failed.`);
        }
        if (!(await services.databaseEvents.onDatabaseInitialised(true))) {
            throw new Error("The local database completion hooks failed during rebuild preparation.");
        }
        if (!(await services.fileProcessing.commitPendingFileEvents())) {
            throw new Error("The current file-event batch could not be released for rebuild preparation.");
        }
    }

    private async completePreparedObsidianRebuild(): Promise<boolean> {
        const services = this.rebuildServices;
        services.appLifecycle.resetIsReady();
        if (!services.database.isDatabaseReady()) return false;
        if (!(await services.fileProcessing.commitPendingFileEvents())) return false;
        services.appLifecycle.markIsReady();
        return true;
    }

    private async tryResetRemoteDatabase(): Promise<void> {
        const services = this.rebuildServices;
        const currentReplicator = services.replicator.getActiveReplicator();
        const settings = services.setting.currentSettings();
        if (!currentReplicator) {
            this._log("No active replicator found when trying to reset remote database.", LOG_LEVEL_NOTICE);
            return;
        }
        if (!("tryResetRemoteDatabase" in currentReplicator) || typeof currentReplicator.tryResetRemoteDatabase !== "function") {
            throw new Error("The active replicator does not support resetting a central remote database.");
        }
        try {
            await currentReplicator.tryResetRemoteDatabase(settings);
        } catch (ex) {
            this._log(ex, LOG_LEVEL_VERBOSE);
            throw ex;
        }
    }
}
