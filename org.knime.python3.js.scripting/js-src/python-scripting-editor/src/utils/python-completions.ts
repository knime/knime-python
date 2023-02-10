import * as monaco from 'monaco-editor';
import type { InputPortInfo, InputTableInfo, InputObjectInfo } from './python-scripting-service';
import type { FlowVariable } from 'scripting-editor/src/utils/scripting-service';

const columnCompletionFor = (
    tableIdx: number,
    tableVarName: string,
    column: string
) => ['pyarrow', 'pandas'].map((format) => {
    const text = `${tableVarName}.to_${format}()["${column}"]`;
    return {
        label: text,
        kind: monaco.languages.CompletionItemKind.Snippet,
        documentation: `Access the column "${column}" of input table ${tableIdx} using ${format}.`,
        insertText: text
    };
});

const tableCompletionFor = (tableIdx: number, tableInfo: InputTableInfo) => [
    {
        label: tableInfo.variableName,
        kind: monaco.languages.CompletionItemKind.Snippet,
        documentation: `Access table ${tableIdx}.`,
        insertText: tableInfo.variableName
    },
    ...['pyarrow', 'pandas'].map((format) => {
        const text = `${tableInfo.variableName}.to_${format}()`;
        return {
            label: text,
            kind: monaco.languages.CompletionItemKind.Snippet,
            documentation: `Access table ${tableIdx} using ${format}.`,
            insertText: text
        };
    }),
    ...tableInfo.columnNames.flatMap((column) => columnCompletionFor(tableIdx, tableInfo.variableName, column))
];

const objectCompletionFor = (objectIdx: number, objectInfo: InputObjectInfo) => {
    const documentation = `Access the object at index ${objectIdx}.

Current type : \`${objectInfo.objectType}\`  
Current value : \` ${objectInfo.objectRepr} \`
    `;
    return {
        label: objectInfo.variableName,
        kind: monaco.languages.CompletionItemKind.Snippet,
        documentation: { value: documentation }, // NB: Implement IMarkdownString
        insertText: objectInfo.variableName
    };
};


const flowVariableCompletion = (flowIndex: number, flowVar: FlowVariable) => ({
    label: `knio.flow_variables['${flowVar.name}']`,
    kind: monaco.languages.CompletionItemKind.Variable,
    documentation: `Access flow variable "${flowVar.name}" of value ${flowVar.value} .`,
    insertText: `knio.flow_variables['${flowVar.name}']`
});
   

export const registerMonacoInputFlowVariableCompletions = (inputFlowVariables: FlowVariable[]) => {
    // Pre-compute the completions but without a range
    type CompletionItemWithoutRange = Omit<monaco.languages.CompletionItem, 'range'>;
    let objectIdx = 0;
    const completions = inputFlowVariables?.flatMap((flowVariable): CompletionItemWithoutRange[] => {
        const objectInfo = flowVariable as FlowVariable;
        return [flowVariableCompletion(objectIdx++, objectInfo)];
    });

    // Register the completion provider using the computed completions
    monaco.languages.registerCompletionItemProvider('python', {
        provideCompletionItems(model, position) {
            const word = model.getWordUntilPosition(position);
            const range = {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: word.startColumn,
                endColumn: word.endColumn
            };
            return { suggestions: completions.map((c) => ({ range, ...c })) };
        }
    });
};


// eslint-disable-next-line no-warning-comments
/*
TODO(AP-20083): abstract Monaco completions into knime-scripting-editor
should look smth like code below. would get rid of monaco dependency outside into knime-scripting-editor.

 - implement interface for completions

export const registerMonacoInputColumnCompletions =
    (completionRule: (input:  ) =>
        ,inputCompletions: ScriptingCompletion[]) => {..}
*/

export const registerMonacoInputColumnCompletions = (inputPortInfos: InputPortInfo[]) => {
    // Pre-compute the completions but without a range
    type CompletionItemWithoutRange = Omit<monaco.languages.CompletionItem, 'range'>;
    let tableIdx = 0;
    let objectIdx = 0;
    const completions = inputPortInfos?.flatMap((portInfo): CompletionItemWithoutRange[] => {
        if (portInfo.type === 'table') {
            const tableInfo = portInfo as InputTableInfo;
            return tableCompletionFor(tableIdx++, tableInfo);
        } else {
            const objectInfo = portInfo as InputObjectInfo;
            return [objectCompletionFor(objectIdx++, objectInfo)];
        }
    });

    // Register the completion provider using the computed completions
    monaco.languages.registerCompletionItemProvider('python', {
        provideCompletionItems(model, position) {
            const word = model.getWordUntilPosition(position);
            const range = {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: word.startColumn,
                endColumn: word.endColumn
            };
            return { suggestions: completions.map((c) => ({ range, ...c })) };
        }
    });
};
