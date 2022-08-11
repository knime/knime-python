import knime_gateway as kg

for _ in range(1000):
    print("something")

print("done chatting")


class DummyEntryPoint(kg.EntryPoint):
    class Java:
        implements = ["org.knime.python3.DefaultPythonGatewayTest.DummyEntryPoint"]


kg.connect_to_knime(DummyEntryPoint())
