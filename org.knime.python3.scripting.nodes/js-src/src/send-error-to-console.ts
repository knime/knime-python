import { consoleHandler } from "@knime/scripting-editor";

export const sendErrorToConsole = ({
  description,
  traceback,
}: {
  description: string;
  traceback?: string[];
}) => {
  if (
    typeof traceback === "undefined" ||
    traceback === null ||
    traceback.length === 0
  ) {
    consoleHandler.writeln({
      error: description,
    });
  } else {
    consoleHandler.writeln({
      error: `${description}\n${traceback?.join("\n")}`,
    });
  }
};
