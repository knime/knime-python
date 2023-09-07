import type { Workspace } from "./types/common";
import { reactive } from "vue";

export type WorkspaceStore = {
  workspace?: Workspace;
};

const workspace: WorkspaceStore = reactive<WorkspaceStore>({});

export const useWorkspaceStore = (): WorkspaceStore => {
  return workspace;
};
