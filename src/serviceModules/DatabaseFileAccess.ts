import type { DatabaseFileAccess } from "@vrtmrz/livesync-commonlib/compat/interfaces/DatabaseFileAccess";
import type { MetaEntry } from "@vrtmrz/livesync-commonlib/compat/common/types";
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

    private async normaliseStoredMetadataSizeByMeta(meta: MetaEntry): Promise<boolean> {
        if (meta.deleted || meta._deleted || !meta._rev) {
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
