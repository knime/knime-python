import knime_gateway as kg

print("startup")


class PrintingEntryPoint(kg.EntryPoint):
    def print(self, msg):
        print(msg)

    class Java:
        implements = ["org.knime.python3.DefaultPythonGatewayTest.PrintingEntryPoint"]


kg.connect_to_knime(PrintingEntryPoint())
