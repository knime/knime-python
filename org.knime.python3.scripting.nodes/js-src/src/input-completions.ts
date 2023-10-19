import * as monaco from "monaco-editor";

const SORT_TEXT_LENGTH = 10;

type InputStringCompletion = {
  label: string;
  detail: string;
};

const getQuoteFn = (
  beginningQuote: '"' | "'" | false,
  endingQuote: '"' | "'" | false,
) => {
  let addBeginningQuote = "";
  if (!beginningQuote) {
    if (endingQuote) {
      // add the same quote as we already have at the end
      addBeginningQuote = endingQuote;
    } else {
      // add a double quote if we have no quotes
      addBeginningQuote = '"';
    }
  }

  let addEndingQuote = "";
  if (addBeginningQuote && addBeginningQuote !== endingQuote) {
    // add the same quote as we add at the beginning
    addEndingQuote = addBeginningQuote;
  } else if (beginningQuote && beginningQuote !== endingQuote) {
    // add the same quote as we already have at the beginning
    addEndingQuote = beginningQuote;
  }
  return (text: string) => `${addBeginningQuote}${text}${addEndingQuote}`;
};

export const registerInputCompletions = (inputs: InputStringCompletion[]) => {
  return monaco.languages.registerCompletionItemProvider("python", {
    provideCompletionItems: (model, position) => {
      // Get the matching column names
      const word = model.getWordUntilPosition(position);
      const matchingInputs = inputs.filter((inp) =>
        inp.label.startsWith(word.word),
      );

      // Quote the insert text if the user has no quotes already
      const getQuotationMark = (before: boolean) => {
        const char = model.getValueInRange({
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: before ? word.startColumn - 1 : word.endColumn,
          endColumn: before ? word.startColumn : word.endColumn + 1,
        });
        return char === '"' || char === "'" ? char : false;
      };
      const quoteInsertText = getQuoteFn(
        getQuotationMark(true),
        getQuotationMark(false),
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
