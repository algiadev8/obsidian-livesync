import type { DatabaseFileAccess } from "@vrtmrz/livesync-commonlib/compat/interfaces/DatabaseFileAccess";
import type { UXFileInfo } from "@vrtmrz/livesync-commonlib/compat/common/types";
import { readAsBlob } from "@vrtmrz/livesync-commonlib/compat/common/utils";
import {
    ServiceDatabaseFileAccessBase,
    type ServiceDatabaseFileAccessDependencies,
} from "@vrtmrz/livesync-commonlib/compat/serviceModules/ServiceDatabaseFileAccessBase";

// markChangesAreSame uses persistent data implicitly, we should refactor it too.
// For now, to make the refactoring done once, we just use them directly.
// Hence it remains in the plug-in rather than Commonlib. (markChangesAreSame is using indexedDB).
// Refactored, now migrating...
export class ServiceDatabaseFileAccess extends ServiceDatabaseFileAccessBase implements DatabaseFileAccess {
    constructor(private readonly databaseFileAccessServices: ServiceDatabaseFileAccessDependencies) {
        super(databaseFileAccessServices);
    }

    override async createChunks(file: UXFileInfo, force?: boolean, skipCheck?: boolean): Promise<boolean> {
        return await super.createChunks(this.normaliseFileSize(file), force, skipCheck);
    }

    override async store(file: UXFileInfo, force?: boolean, skipCheck?: boolean): Promise<boolean> {
        const normalised = this.normaliseFileSize(file);
        const result = await super.store(normalised, force, skipCheck);
        if (result) {
            await this.normaliseStoredMetadataSize(normalised);
        }
        return result;
    }

    override async storeWithBaseRevision(
        file: UXFileInfo,
        baseRevision: string | undefined,
        skipCheck?: boolean
    ): Promise<string | false> {
        const normalised = this.normaliseFileSize(file);
        const result = await super.storeWithBaseRevision(normalised, baseRevision, skipCheck);
        if (result !== false) {
            return await this.normaliseStoredMetadataSize(normalised, result);
        }
        return result;
    }

    override async storeWithLiveBaseRevision(
        file: UXFileInfo,
        baseRevision: string,
        skipCheck?: boolean
    ): Promise<string | false> {
        const normalised = this.normaliseFileSize(file);
        const result = await super.storeWithLiveBaseRevision(normalised, baseRevision, skipCheck);
        if (result !== false) {
            return await this.normaliseStoredMetadataSize(normalised, result);
        }
        return result;
    }

    override async storeAsConflictedRevision(
        file: UXFileInfo,
        currentRev: string,
        skipCheck?: boolean
    ): Promise<boolean> {
        return await super.storeAsConflictedRevision(this.normaliseFileSize(file), currentRev, skipCheck);
    }

    override async storeAsConflictedRevisionWithResult(
        file: UXFileInfo,
        currentRev: string,
        skipCheck?: boolean
    ): Promise<string | false> {
        return await super.storeAsConflictedRevisionWithResult(this.normaliseFileSize(file), currentRev, skipCheck);
    }

    private normaliseFileSize(file: UXFileInfo): UXFileInfo {
        if (file.stat.size === file.body.size) {
            return file;
        }
        return {
            ...file,
            stat: {
                ...file.stat,
                size: file.body.size,
            },
        };
    }

    private async normaliseStoredMetadataSize(file: UXFileInfo, revision?: string): Promise<string> {
        const meta = await this.fetchEntryMeta(file, revision, true);
        if (!meta || meta.deleted) {
            return revision ?? "";
        }
        const loaded = await this.fetchEntryFromMeta(meta, true, true);
        if (!loaded) {
            return revision ?? meta._rev;
        }
        const actualSize = readAsBlob(loaded).size;
        if (meta.size === actualSize) {
            return meta._rev;
        }
        const raw = await this.databaseFileAccessServices.database.localDatabase.localDatabase.get(meta._id, {
            rev: meta._rev,
        });
        const result = await this.databaseFileAccessServices.database.localDatabase.putRaw({
            ...raw,
            size: actualSize,
        });
        return result.rev;
    }
}
