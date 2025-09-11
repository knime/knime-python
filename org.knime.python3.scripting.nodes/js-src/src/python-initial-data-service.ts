import {
  type GenericInitialData,
  type InputOutputModel,
  getInitialData,
} from "@knime/scripting-editor";

import type { ExecutableOption } from "./types/common";

export type PythonInitialData = GenericInitialData & {
  hasPreview: boolean;
  outputObjects: InputOutputModel[];
  executableOptionsList: ExecutableOption[];
};

// TODO no "service" is needed anymore, just export getInitialData directly
export const getPythonInitialDataService = () => ({
  getInitialData: () => getInitialData() as PythonInitialData,
});
