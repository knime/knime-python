package org.knime.python.kernel;

import java.util.List;

import org.knime.python.kernel.proto.ProtobufKnimeHiveOutput.HiveOutput;
import org.knime.python.kernel.proto.ProtobufKnimeSQLOutput.SQLOutput;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetSQL;
import org.knime.python.kernel.proto.ProtobufPythonKernelCommand.Command.GetSQL.Builder;

import com.google.protobuf.InvalidProtocolBufferException;

public class SQLEditorObjectReader implements EditorObjectReader {

    private final String m_name;
    private String m_query;
    private boolean m_hasHive = false;
    private List<String> m_columnNamesList;
    private List<String> m_columnTypeList;
    private boolean m_tableExist;
    private boolean m_dropTable;
    private List<String> m_partitionColumnNamesList;
    private String m_delimiter;
    private String m_fileName;
    private String m_tableName;

    public SQLEditorObjectReader(final String name) {
        m_name = name;
    }

    @Override
    public Command getCommand() {
        final Command.Builder commandBuilder = Command.newBuilder();
        final Builder getSql = GetSQL.newBuilder();
        getSql.setKey(m_name);
        commandBuilder.setGetSQL(getSql);
        final Command command = commandBuilder.build();
        return command;
    }

    @Override
    public void read(final byte[] readMessageBytes) throws InvalidProtocolBufferException {
        final SQLOutput sql = SQLOutput.parseFrom(readMessageBytes);
        m_query = sql.getQuery();
        if(sql.hasHive()){
            m_hasHive = true;
            final HiveOutput hive = sql.getHive();
            m_columnNamesList = hive.getColumnNamesList();
            m_columnTypeList = hive.getColumnTypeList();
            m_tableExist = hive.getTableExist();
            m_dropTable = hive.getDropTable();
            m_partitionColumnNamesList = hive.getPartitionColumnNamesList();
            m_delimiter = hive.getDelimiter();
            m_fileName = hive.getFileName();
            m_tableName = hive.getTableName();
        }
    }

    public String getTableName() {
        return m_tableName;
    }

    public boolean isTableExist() {
        return m_tableExist;
    }

    public boolean isDropTable() {
        return m_dropTable;
    }

    public List<String> getPartitionColumnNamesList() {
        return m_partitionColumnNamesList;
    }

    public String getFileName() {
        return m_fileName;
    }

    public List<String> getColumnNamesList() {
        return m_columnNamesList;
    }

    public List<String> getColumnTypeList() {
        return m_columnTypeList;
    }

    public String getQuery() {
        return m_query;
    }

    public boolean getHasHive() {
        return m_hasHive;
    }

    public String getDelimiter() {
        return m_delimiter;
    }

}
