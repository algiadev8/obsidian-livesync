import { ServiceFileAccessBase } from "@vrtmrz/livesync-commonlib/compat/serviceModules/ServiceFileAccessBase";
import type { UXFileInfo, UXFileInfoStub } from "@vrtmrz/livesync-commonlib/compat/common/types";
import type { ObsidianFileSystemAdapter } from "./FileSystemAdapters/ObsidianFileSystemAdapter";

export class ServiceFileAccessObsidian extends ServiceFileAccessBase<ObsidianFileSystemAdapter> {
    override async readStubContent(stub: UXFileInfoStub): Promise<UXFileInfo | false> {
        const file = await super.readStubContent(stub);
        if (!file) return false;
        return {
            ...file,
            stat: {
                ...file.stat,
                size: file.body.size,
            },
        };
    }
}
