import knime_gateway as kg


class DummyEntryPoint(kg.EntryPoint):
    class Java:
        implements = ["org.knime.python3.DefaultPythonGatewayTest.DummyEntryPoint"]


# this was the mistake I got stuck with
print(any(["foo"], lambda x: x == "foo"))

kg.connect_to_knime(DummyEntryPoint())
