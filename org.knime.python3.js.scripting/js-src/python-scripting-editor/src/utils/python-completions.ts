import * as monaco from 'monaco-editor';
import { InputPortInfo, InputTableInfo } from './python-scripting-service';

const columnCompletionFor = (
    tableIdx: number,
    column: string,
    range: monaco.IRange
): monaco.languages.CompletionItem[] => ['pyarrow', 'pandas'].map((format) => {
    const text = `knio.input_table[${tableIdx}].to_${format}()["${column}"]`;
    return {
        label: text,
        kind: monaco.languages.CompletionItemKind.Snippet,
        documentation: `Access the column "${column}" of input table ${tableIdx} using ${format}.`,
        insertText: text,
        range
    };
});

const tableCompletionFor = (
    tableIdx: number,
    table: string[],
    range: monaco.IRange
): monaco.languages.CompletionItem[] => {
    const tableAccess = `knio.input_table[${tableIdx}]`;
    return [
        {
            label: tableAccess,
            kind: monaco.languages.CompletionItemKind.Snippet,
            documentation: `Access table ${tableIdx}.`,
            insertText: tableAccess,
            range
        },
        ...['pyarrow', 'pandas'].map((format) => {
            const text = `${tableAccess}.to_${format}()`;
            return {
                label: text,
                kind: monaco.languages.CompletionItemKind.Snippet,
                documentation: `Access table ${tableIdx} using ${format}.`,
                insertText: text,
                range
            };
        }),
        ...table.flatMap((column) => columnCompletionFor(tableIdx, column, range))
    ];
};

export const registerMonacoInputColumnCompletions = (inputPortInfos: InputPortInfo[]) => {
    // TODO make autocompletion faster by pre-computing the options and only adding the range to them
    monaco.languages.registerCompletionItemProvider('python', {
        provideCompletionItems(model, position) {
            const word = model.getWordUntilPosition(position);
            const range = {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: word.startColumn,
                endColumn: word.endColumn
            };

            return {
                suggestions: inputPortInfos.flatMap((portInfo, tableIdx) => {
                    if (portInfo.type === 'table') {
                        const tableInfo = portInfo as InputTableInfo;
                        return tableCompletionFor(tableIdx, tableInfo.columnNames, range);
                    } else {
                        // TODO add autocompletion for object ports
                        return [];
                    }
                })
            };
        }
    });
};
