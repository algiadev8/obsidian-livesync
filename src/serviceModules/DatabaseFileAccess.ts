import type { DatabaseFileAccess } from "@vrtmrz/livesync-commonlib/compat/interfaces/DatabaseFileAccess";
import type { UXFileInfo } from "@vrtmrz/livesync-commonlib/compat/common/types";
import { ServiceDatabaseFileAccessBase } from "@vrtmrz/livesync-commonlib/compat/serviceModules/ServiceDatabaseFileAccessBase";

// markChangesAreSame uses persistent data implicitly, we should refactor it too.
// For now, to make the refactoring done once, we just use them directly.
// Hence it remains in the plug-in rather than Commonlib. (markChangesAreSame is using indexedDB).
// Refactored, now migrating...
export class ServiceDatabaseFileAccess extends ServiceDatabaseFileAccessBase implements DatabaseFileAccess {
    override async createChunks(file: UXFileInfo, force?: boolean, skipCheck?: boolean): Promise<boolean> {
        return await super.createChunks(this.normaliseFileSize(file), force, skipCheck);
    }

    override async store(file: UXFileInfo, force?: boolean, skipCheck?: boolean): Promise<boolean> {
        return await super.store(this.normaliseFileSize(file), force, skipCheck);
    }

    override async storeWithBaseRevision(
        file: UXFileInfo,
        baseRevision: string | undefined,
        skipCheck?: boolean
    ): Promise<string | false> {
        return await super.storeWithBaseRevision(this.normaliseFileSize(file), baseRevision, skipCheck);
    }

    override async storeWithLiveBaseRevision(
        file: UXFileInfo,
        baseRevision: string,
        skipCheck?: boolean
    ): Promise<string | false> {
        return await super.storeWithLiveBaseRevision(this.normaliseFileSize(file), baseRevision, skipCheck);
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
}
