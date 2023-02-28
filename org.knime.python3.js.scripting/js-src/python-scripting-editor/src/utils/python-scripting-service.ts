import type { FlowVariableSettings, FlowVariableSetting } from '@knime/ui-extension-service';
import type { ScriptingService, NodeSettings } from 'scripting-editor/src/utils/scripting-service';
import { ScriptingServiceImpl,
    muteReactivity,
    useKnimeScriptingService } from 'scripting-editor/src/utils/scripting-service';

export type Workspace = { names: string[]; types: string[]; values: string[] };

export type InputPortBase = { type: 'table' | 'object'; variableName: string };
export interface InputTableInfo extends InputPortBase {
    type: 'table';
    columnNames: string[];
    columnTypes: string[];
}
export interface InputObjectInfo extends InputPortBase {
    type: 'object';
    objectType: string;
    objectRepr: string;
}

export type InputPortInfo = InputTableInfo | InputObjectInfo

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

export interface PythonScriptingService extends ScriptingService<PythonNodeSettings> {
    dialogOpened();
    
    initExecutableOptions();

    sendLastConsoleOutput();

    startInteractive(executableSelection: string);

    runInteractive(script: string);

    getInputObjects();

    getInitialExecutableSelection();

    getExecutableSelection();

    setExecutableSelection(id: string);

    getExecutableOptions(executableSelection: string);
    
    getExecutableInfo(id: string);
}

class PythonScriptingServiceImpl extends ScriptingServiceImpl<PythonNodeSettings> implements PythonScriptingService {
    dialogOpened(): Promise<void> {
        return this.sendToService('openedDialog');
    }
    
    initExecutableOptions(): Promise<void> {
        //
        return this.sendToService('initExecutableOptions');
    }


    sendLastConsoleOutput(): Promise<void> {
        return this.sendToService('sendLastConsoleOutput');
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
    flowVariableSettings,
}: {
    flowVariableSettings: FlowVariableSettings;
}) => {
    let executableSelection = '';
    if ('model.python3_command' in flowVariableSettings) {
        const commandFlowVarSetting: FlowVariableSetting = flowVariableSettings.modelVariables['model.python3_command'];
        if (commandFlowVarSetting.controllingFlowVariableName !== null) {
            executableSelection = commandFlowVarSetting.controllingFlowVariableName;
        }
    }
    return executableSelection;
};

export const createScriptingService = async () => {
    const { jsonDataService,
        flowVariableSettings } = await useKnimeScriptingService();

    const initialNodeSettings = await jsonDataService.initialData();

    const pythonScriptingService: PythonNodeSettings = {
        ...initialNodeSettings,
        executableSelection: overwritePythonCommandByFlowVarName({ flowVariableSettings }),
    };
    const scriptingService = new PythonScriptingServiceImpl(
        jsonDataService, flowVariableSettings, pythonScriptingService,
    );
    muteReactivity(scriptingService);
    return scriptingService as PythonScriptingService;
};
