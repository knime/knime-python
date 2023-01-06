import type { ConsoleText } from 'scripting-editor/src/utils/scripting-service';

import type { ExecutableOption,
    InputTableInfo,
    PythonNodeSettings,
    CondaPackageInfo,
    InputPortInfo,
    ExecutableInfo } from './python-scripting-service';

import type { FlowVariableSetting } from '@knime/ui-extension-service';

export class PythonScriptingService {
    protected readonly flowVariableSettings: {[key: string]: FlowVariableSetting}; // TODO(UIEXT-479) refactor how flow variable information are provided
    // protected readonly initialNodeSettings: T;
    protected currentNodeSettings: PythonNodeSettings;
    protected initialNodeSettings: PythonNodeSettings;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    eventHandlers: { [type: string]: (args: any) => void };

    constructor({
        flowVariableSettings,
        initialNodeSettings
    }: {
        flowVariableSettings: {[key: string]: FlowVariableSetting};
        initialNodeSettings: PythonNodeSettings
    }) {
        this.flowVariableSettings = flowVariableSettings;
        this.initialNodeSettings = initialNodeSettings;
        this.eventHandlers = {};
        this.currentNodeSettings = {
            ...initialNodeSettings
        };
    }
    /* eslint-disable */
    dialogOpened(): Promise<void> {
        return new Promise((r) => setTimeout(r, 100)).then(() => {
            console.log('dialogOpened');
        });
    }
    
    initExecutableOptions(): Promise<void> {
        //
        return new Promise((r) => setTimeout(r, 100)).then(() => {
            console.log('initExecutableOptions');
        });
    }

    sendLastConsoleOutput(): Promise<void> {
        return new Promise((r) => setTimeout(r, 100)).then(() => {
            console.log('sendLastConsoleOutput');
        });
    }

    startInteractive(executableSelection: string): Promise<void> {
        return new Promise((r) => setTimeout(r, 100)).then(() => {
            console.log('start interactive');
        });
    }

    runInteractive(script: string): Promise<string> {
        // Trigger both Console and Workspace change.
        const res = {
            names: ['x'],
            types: ['int'],
            values: ['3']
        };
        this.eventHandlers.console({
            text: 'x=3',
            stderr: false
        } as ConsoleText);
        return new Promise((r) => setTimeout(r, 100)).then(() => JSON.stringify(
            res
        ));
    }

    getInputObjects(): Promise<InputPortInfo[]> {
        return new Promise((r) => setTimeout(r, 100)).then(() => [
            {
                variableName: 'MockTable',
                type: 'table',
                columnNames: ['T1'],
                columnTypes: ['int']
            } as InputTableInfo
        ]);
    }

    applySettings(): Promise<any> {
        return new Promise((r) => setTimeout(r, 100));
    }

    // TODO is this implemented in the backend?
    async applySettingsAndExecute() {
        await this.applySettings();
    }

    getInitialExecutableSelection(): string {
        return '1'; // this.initialNodeSettings.executableSelection;
    }

    getExecutableSelection(): string {
        return '1';// this.currentNodeSettings.executableSelection;
    }

    setExecutableSelection(id: string) {
        this.currentNodeSettings.executableSelection = id;
    }

    getInitialScript(): string {
        return 'import numpy as np\n';
    }

    setScript(script: string): void {
        console.log('setscript', script);
    }
    /* eslint-enable */

    startLanguageClient(name: string, documentSelector?: string[]) : Promise<any> {
        return new Promise((r) => setTimeout(r, 100));
    }

    getExecutableOptions(executableSelection: string): Promise<ExecutableOption[]> {
        return Promise.resolve([{
            type: 'STRING_VAR',
            id: '1',
            pythonExecutable: '/home/',
            condaEnvName: 'hallo',
            condaEnvDir: '/home/'
        } as ExecutableOption]);
    }

    getExecutableInfo(id: string): Promise<ExecutableInfo> {
        return Promise.resolve({
            pythonVersion: '3.10',
            packages: [{
                name: 'name',
                version: 'version',
                build: 'build',
                channel: 'channel'
            } as CondaPackageInfo]
        } as ExecutableInfo);
    }

    registerConsoleEventHandler(handler: (text: ConsoleText) => void) {
        this.eventHandlers.console = (e) => new Promise((r) => setTimeout(r, 100)).then(() => {
            handler(e);
        });
    }
}

export const createScriptingService = async () => {
    await new Promise((r) => setTimeout(r, 1000));
    const scriptingService = new PythonScriptingService({
        flowVariableSettings: {},
        initialNodeSettings: {
            script: '#This is mocking example',
            executableSelection: '1'
        }
    });
    return scriptingService;
};
