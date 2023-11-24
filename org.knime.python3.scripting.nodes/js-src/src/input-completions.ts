import * as monaco from "monaco-editor";

const SORT_TEXT_LENGTH = 10;

type InputStringCompletion = {
  label: string;
  detail: string;
};

const getQuoteFn = (
  beginningChar: '"' | "'" | "." | false,
  endingChar: '"' | "'" | "." | false,
) => {
  if (beginningChar === ".") {
    // Do not add any quotes if we have a dot
    return (text: string) => text;
  }

  let addBeginningQuote = "";
  if (!beginningChar) {
    if (endingChar) {
      // add the same quote as we already have at the end
      addBeginningQuote = endingChar;
    } else {
      // add a double quote if we have no quotes
      addBeginningQuote = '"';
    }
  }

  let addEndingQuote = "";
  if (addBeginningQuote && addBeginningQuote !== endingChar) {
    // add the same quote as we add at the beginning
    addEndingQuote = addBeginningQuote;
  } else if (beginningChar && beginningChar !== endingChar) {
    // add the same quote as we already have at the beginning
    // do not do anything if we have a dot
    addEndingQuote = beginningChar;
  }
  return (text: string) => `${addBeginningQuote}${text}${addEndingQuote}`;
};

export const registerInputCompletions = (inputs: InputStringCompletion[]) => {
  return monaco.languages.registerCompletionItemProvider("python", {
    provideCompletionItems: (model, position) => {
      // Get the matching column names
      const word = model.getWordUntilPosition(position);
      const matchingInputs = inputs.filter((inp) =>
        inp.label.toLocaleLowerCase().startsWith(word.word.toLocaleLowerCase()),
      );

      // Quote the insert text if the user has no quotes already
      const getAdjacentChar = (before: boolean) => {
        const char = model.getValueInRange({
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: before ? word.startColumn - 1 : word.endColumn,
          endColumn: before ? word.startColumn : word.endColumn + 1,
        });
        return char === '"' || char === "'" || char === "." ? char : false;
      };
      const quoteInsertText = getQuoteFn(
        getAdjacentChar(true),
        getAdjacentChar(false),
      );

      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      return {
        suggestions: matchingInputs.map((inp, idx) => ({
          label: inp.label,
          detail: inp.detail,
          insertText: quoteInsertText(inp.label),
          sortText: `${idx}`.padStart(SORT_TEXT_LENGTH, "0"),
          kind: monaco.languages.CompletionItemKind.Snippet,
          range,
        })),
      };
    },
  });
};
