import { JsonDataService } from 'knime-ui-extension-service';
import { FlowVariableSetting } from 'knime-ui-extension-service/src/types/FlowVariableSettings';
import { ScriptingService,
    muteReactivity,
    createJsonServiceAndLoadSettings,
    NodeSettings } from 'scripting-editor/src/utils/scripting-service';

export type Workspace = { names: string[]; types: string[]; values: string[] };

// Types for the input port view
export type InputPortInfo = { type: 'table' | 'object'; variableName: string };
export interface InputTableInfo extends InputPortInfo {
    type: 'table';
    columnNames: string[];
    columnTypes: string[];
}
export interface InputObjectInfo extends InputPortInfo {
    type: 'object';
    objectType: string;
    objectRepr: string;
}

// Types for the executable selection
export type ExecutableOption = {
    type: 'PREF_BUNDLED' | 'PREF_CONDA' | 'PREF_MANUAL' | 'CONDA_ENV_VAR' | 'STRING_VAR' | 'MISSING_VAR';
    id: string;
    pythonExecutable: string | null;
    condaEnvName: string | null;
    condaEnvDir: string | null;
};
export type CondaPackageInfo = { name: string; version: string; build: string; channel: string };
export type ExecutableInfo = { pythonVersion: string | null; packages: CondaPackageInfo[] };

export interface PythonNodeSettings extends NodeSettings {
    executableSelection: string;
}

export class PythonScriptingService extends ScriptingService<PythonNodeSettings> {
    dialogOpened(): Promise<void> {
        return this.sendToService('openedDialog');
    }

    startInteractive(executableSelection: string): Promise<void> {
        return this.sendToService('startInteractive', [executableSelection]);
    }

    runInteractive(script: string): Promise<string> {
        return this.sendToService('runInteractive', [script]);
    }

    getInputObjects(): Promise<InputPortInfo[]> {
        return this.sendToService('getInputObjects');
    }

    getInitialExecutableSelection(): string {
        return this.initialNodeSettings.executableSelection;
    }

    getExecutableSelection(): string {
        return this.currentNodeSettings.executableSelection;
    }

    setExecutableSelection(id: string) {
        this.currentNodeSettings.executableSelection = id;
    }

    getExecutableOptions(executableSelection: string): Promise<ExecutableOption[]> {
        return this.sendToService('getExecutableOptions', [executableSelection]);
    }

    getExecutableInfo(id: string): Promise<ExecutableInfo> {
        return this.sendToService('getExecutableInfo', [id]);
    }
}

const overwritePythonCommandByFlowVarName = ({
    jsonDataService,
    flowVariableSettings,
    initialNodeSettings
}: {
    jsonDataService: JsonDataService;
    flowVariableSettings: { [key: string]: FlowVariableSetting };
    initialNodeSettings: PythonNodeSettings;
}) => {
    let executableSelection = '';
    if ('model.python3_command' in flowVariableSettings) {
        const commandFlowVarSetting: FlowVariableSetting = flowVariableSettings['model.python3_command'];
        if (commandFlowVarSetting.controllingFlowVariableName !== null) {
            executableSelection = commandFlowVarSetting.controllingFlowVariableName;
        }
    }
    return {
        jsonDataService,
        flowVariableSettings,
        initialNodeSettings: {
            script: initialNodeSettings.script,
            executableSelection
        }
    };
};

export const createScriptingService = async () => {
    const scriptingService = new PythonScriptingService(
        overwritePythonCommandByFlowVarName(await createJsonServiceAndLoadSettings())
    );
    muteReactivity(scriptingService);
    return scriptingService;
};
