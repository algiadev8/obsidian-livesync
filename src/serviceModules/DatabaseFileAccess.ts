import type { DatabaseFileAccess } from "@vrtmrz/livesync-commonlib/compat/interfaces/DatabaseFileAccess";
import type { FilePathWithPrefix, UXFileInfo } from "@vrtmrz/livesync-commonlib/compat/common/types";
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

    async normaliseAllStoredMetadataSizes(): Promise<{ checked: number; updated: number }> {
        let checked = 0;
        let updated = 0;
        for await (const meta of this.databaseFileAccessServices.database.localDatabase.findAllNormalDocs({
            conflicts: true,
        })) {
            if (meta.deleted || meta._deleted) {
                continue;
            }
            checked++;
            if (await this.normaliseStoredMetadataSizeByMeta(meta)) {
                updated++;
            }
        }
        return { checked, updated };
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
        const updated = await this.normaliseStoredMetadataSizeByMeta(meta);
        if (!updated) {
            return meta._rev;
        }
        const current = await this.fetchEntryMeta(file, undefined, true);
        return current ? current._rev : meta._rev;
    }

    private async normaliseStoredMetadataSizeByMeta(meta: {
        _id: string;
        _rev: string;
        path: FilePathWithPrefix;
        size: number;
        deleted?: boolean;
        _deleted?: boolean;
    }): Promise<boolean> {
        if (meta.deleted || meta._deleted) {
            return false;
        }
        const loaded = await this.fetchEntryFromMeta(meta, true, true);
        if (!loaded) {
            return false;
        }
        const actualSize = readAsBlob(loaded).size;
        if (meta.size === actualSize) {
            return false;
        }
        const raw = await this.databaseFileAccessServices.database.localDatabase.localDatabase.get(meta._id, {
            rev: meta._rev,
        });
        await this.databaseFileAccessServices.database.localDatabase.putRaw({
            ...raw,
            size: actualSize,
        });
        return true;
    }
}
