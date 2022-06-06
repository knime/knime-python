import knime_gateway as kg


class DummyEntryPoint(kg.EntryPoint):

    class Java:
        implements = ["org.knime.python3.DefaultPythonGatewayTest.DummyEntryPoint"]

kg.connect_to_knime(DummyEntryPoint())