/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 27, 2025 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.python3.views;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.knime.core.webui.data.RpcDataService.WildcardHandler;
import org.knime.python3.views.HtmlFileNodeView.JsonRpcRequestHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A {@link WildcardHandler} that uses JSON-RPC to handle requests.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class JsonRpcWildcardHandler implements WildcardHandler {

    private record JsonRpcRequest(//
        String jsonrpc, //
        String method, //
        Object params, //
        int id) {
    }

    private record JsonRpcResponse(//
        String jsonrpc, //
        Object result, //
        JsonRpcError error, //
        int id) {
    }

    private record JsonRpcError(//
        int code, //
        String message) {

        void throwForStatus() throws RequestException {
            var exceptionConstructor = ERROR_CODE_MAP.get(code);
            if (exceptionConstructor != null) {
                throw exceptionConstructor.apply(message);
            } else {
                throw new IllegalStateException(
                    "Unexpected exception encountered. Code: %s Message: %s".formatted(code, message));
            }
        }
    }

    // parse error and invalid request handler are not explicitly handled as we expect
    // the frontend and the backend to provide valid requests and responses.
    private static final int METHOD_NOT_FOUND = -32601;

    private static final int INVALID_PARAMS = -32602;

    private static final int INTERNAL_ERROR = -32603;

    // Map error codes to exception constructors
    private static final Map<Integer, Function<String, RequestException>> ERROR_CODE_MAP = Map.of(//
        METHOD_NOT_FOUND, WildcardHandler.MethodNotFoundException::new, //
        INVALID_PARAMS, WildcardHandler.InvalidParamsException::new, //
        INTERNAL_ERROR, WildcardHandler.InternalErrorException::new//
    );


    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final JsonRpcRequestHandler m_dataService;

    JsonRpcWildcardHandler(final JsonRpcRequestHandler dataService) {
        m_dataService = dataService;
    }

    @Override
    public Object handleRequest(final String method, final List<Object> params) throws RequestException {
        return handleRequestInternal(method, params);
    }

    @Override
    public Object handleRequest(final String method, final Map<String, Object> params) throws RequestException {
        return handleRequestInternal(method, params);
    }

    private Object handleRequestInternal(final String method, final Object params) throws RequestException {
        var jsonRpcRequest = new JsonRpcRequest("2.0", method, params, 1);
        var jsonRpcResponse = m_dataService.handleRequest(serializeRequest(jsonRpcRequest));
        return deserializeResponse(jsonRpcResponse);
    }

    private static String serializeRequest(final JsonRpcRequest request) {
        try {
            return OBJECT_MAPPER.writeValueAsString(request);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize JSON-RPC request", e);
        }
    }

    private static Object deserializeResponse(final String jsonRpcResponse) throws RequestException {
        var response = parseResponse(jsonRpcResponse);
        var error = response.error();
        if (response.error() != null) {
            error.throwForStatus();
        }
        return response.result();
    }

    private static JsonRpcResponse parseResponse(final String jsonRpcResponse) {
        try {
            return OBJECT_MAPPER.readValue(jsonRpcResponse, JsonRpcResponse.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to parse JSON-RPC response", e);
        }
    }

}