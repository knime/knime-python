export enum ExecutableOptionType {
  PREF_BUNDLED,
  PREF_CONDA,
  PREF_MANUAL,
  CONDA_ENV_VAR,
  STRING_VAR,
  MISSING_VAR,
}

export type ExecutableOption = {
  type: ExecutableOptionType;
  id: string;
  pythonExecutable: string;
  condaEnvName: string;
  condaEnvDir: string;
};

export type InputPortInfo = {
  type: string;
  variableName: string;
};

export type Workspace = {
  name: string;
  type: string;
  value: string;
}[];

export type ExecutionInfoWithTraceback = {
  status: "EXECUTION_ERROR";
  description: string;
  traceback: string[];
  data: Workspace;
};

export type ExecutionInfoWithWorkspace = {
  status: "SUCCESS";
  description: string;
  data: Workspace;
};

export type ExecutionInfo =
  | ExecutionInfoWithTraceback
  | ExecutionInfoWithWorkspace
  | {
      status: "RUNNING" | "CANCELLED" | "KNIME_ERROR" | "FATAL_ERROR";
      description: string;
    };

export type KillSessionInfo = {
  status: "SUCCESS" | "ERROR";
  description: string;
};

export type StartSessionInfo = {
  status: "SUCCESS" | "ERROR";
  description: string;
};

export type SessionInfo = {
  status: string;
  description: string;
};

export const ERROR_STATES = ["KNIME_ERROR", "FATAL_ERROR", "EXECUTION_ERROR"];
