import { REMOTE_P2P } from "@vrtmrz/livesync-commonlib/compat/common/models/setting.const";
import { isDocContentSame, readAsBlob } from "@vrtmrz/livesync-commonlib/compat/common/utils";
import type { Rebuilder } from "@vrtmrz/livesync-commonlib/compat/interfaces/DatabaseRebuilder";
import { ServiceRebuilder, type ServiceRebuilderDependencies } from "@vrtmrz/livesync-commonlib/compat/serviceModules/Rebuilder";
import { shouldBeIgnored } from "@vrtmrz/livesync-commonlib/compat/string_and_binary/path";
import { DEFAULT_SETTINGS } from "@vrtmrz/livesync-commonlib/settings";
import { LOG_LEVEL_NOTICE, LOG_LEVEL_VERBOSE } from "octagonal-wheels/common/logger";
import { delay } from "octagonal-wheels/promises";
import { ServiceDatabaseFileAccess } from "./DatabaseFileAccess.ts";

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
        await this.normaliseLocalDatabaseMetadataSizes();
        await this.verifyLocalDatabaseMatchesStorage();
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
        this._log(
            "Obsidian rebuild: forcing local database from storage files before remote overwrite.",
            LOG_LEVEL_NOTICE,
            "rebuild-storage-authoritative"
        );
        const files = await services.storageAccess.getFiles();
        let processed = 0;
        let skipped = 0;
        let failed = 0;
        for (const file of files) {
            if (shouldBeIgnored(file.path)) {
                this._log(`REBUILD STORAGE -> DB : ${file.path} has been skipped because it is ignored`, LOG_LEVEL_VERBOSE);
                skipped++;
                continue;
            }
            if (!(await services.vault.isTargetFile(file.path))) {
                this._log(`REBUILD STORAGE -> DB : ${file.path} has been skipped because it is not a target file`, LOG_LEVEL_VERBOSE);
                skipped++;
                continue;
            }
            if (services.vault.isFileSizeTooLarge(file.stat.size)) {
                this._log(`REBUILD STORAGE -> DB : ${file.path} has been skipped due to file size exceeding the limit`, LOG_LEVEL_NOTICE);
                skipped++;
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
        this._log(
            `Obsidian rebuild: forced local database preparation completed (${processed} stored, ${skipped} skipped, ${failed} failed).`,
            LOG_LEVEL_NOTICE,
            "rebuild-storage-authoritative"
        );
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

    private async normaliseLocalDatabaseMetadataSizes(): Promise<void> {
        const databaseFileAccess = this.rebuildServices.fileHandler.db;
        if (!(databaseFileAccess instanceof ServiceDatabaseFileAccess)) {
            this._log("Obsidian rebuild: metadata size normalisation was skipped because the database access service was not available.", LOG_LEVEL_NOTICE);
            return;
        }
        const result = await databaseFileAccess.normaliseAllStoredMetadataSizes();
        this._log(
            `Obsidian rebuild: metadata size normalisation completed (${result.checked} checked, ${result.updated} updated).`,
            LOG_LEVEL_NOTICE,
            "rebuild-storage-authoritative"
        );
    }

    private async verifyLocalDatabaseMatchesStorage(): Promise<void> {
        const services = this.rebuildServices;
        const databaseFileAccess = services.fileHandler.db;
        if (!(databaseFileAccess instanceof ServiceDatabaseFileAccess)) {
            throw new Error("The database access service was not available for rebuild verification.");
        }
        const files = await services.storageAccess.getFiles();
        let checked = 0;
        let skipped = 0;
        const mismatched = [] as string[];
        for (const file of files) {
            if (shouldBeIgnored(file.path) || !(await services.vault.isTargetFile(file.path))) {
                skipped++;
                continue;
            }
            if (services.vault.isFileSizeTooLarge(file.stat.size)) {
                skipped++;
                continue;
            }
            const storageFile = await services.storageAccess.readStubContent(file);
            const meta = await databaseFileAccess.fetchEntryMeta(file, undefined, true);
            const loaded = meta ? await databaseFileAccess.fetchEntryFromMeta(meta, true, true) : false;
            checked++;
            if (!storageFile || !loaded || !(await isDocContentSame(storageFile.body, readAsBlob(loaded)))) {
                mismatched.push(file.path);
            }
        }
        this._log(
            `Obsidian rebuild: storage/database verification completed (${checked} checked, ${skipped} skipped, ${mismatched.length} mismatched).`,
            LOG_LEVEL_NOTICE,
            "rebuild-storage-authoritative"
        );
        if (mismatched.length !== 0) {
            throw new Error(`The rebuild was stopped because ${mismatched.length} local database document(s) did not match storage: ${mismatched.slice(0, 10).join(", ")}`);
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
