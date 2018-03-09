/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.ptf;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.PolymorphicTableFunctionFactory;
import com.facebook.presto.spi.function.TableFunction;
import com.facebook.presto.spi.function.TableFunctionDescriptor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class SplitColumnTableFunctionFactory
        implements PolymorphicTableFunctionFactory
{
    private static final JsonCodec<SplitColumnFunctionHandle> CODEC = JsonCodec.jsonCodec(SplitColumnFunctionHandle.class);
    private final TypeManager typeManager;

    public SplitColumnTableFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return "split_column";
    }

    @Override
    public TableFunctionDescriptor describe(Map<String, Object> arguments)
    {
        List<ColumnMetadata> inputs = getDescriptorParameter(arguments, "input");
        List<ColumnMetadata> outputs = getDescriptorParameter(arguments, "output");

        String splitColumn = getParameter(arguments, "split_column", Slice.class, "varchar").toStringUtf8();
        int splitColumnIndex = getColumnIndex(inputs, splitColumn)
                .orElseThrow(() -> new PrestoException(INVALID_FUNCTION_ARGUMENT, "split_column '" + splitColumn + "' not found in input " + inputs));


        SplitColumnFunctionHandle handle = new SplitColumnFunctionHandle(
                splitColumnIndex,
                getParameter(arguments, "delimiter", Slice.class, "varchar").toStringUtf8(),
                outputs.stream()
                        .map(type -> type.getType().getTypeSignature())
                        .collect(toImmutableList()));

        return new TableFunctionDescriptor(
                CODEC.toJsonBytes(handle),
                IntStream.range(0, inputs.size()).boxed().collect(toImmutableList()),
                ImmutableList.<ColumnMetadata>builder()
                        .addAll(inputs)
                        .addAll(outputs)
                        .build());
    }

    private static OptionalInt getColumnIndex(List<ColumnMetadata> inputs, String splitColumn)
    {
        for (int i = 0; i < inputs.size(); i++) {
            ColumnMetadata columnMetadata = inputs.get(i);
            if (columnMetadata.getName().equals(splitColumn)) {
                return OptionalInt.of(i);
            }
        }
        return OptionalInt.empty();
    }

    @Override
    public TableFunction getInstance(byte[] handleJson)
    {
        SplitColumnFunctionHandle handle = CODEC.fromJson(handleJson);

        List<Type> types = handle.getOutputTypes().stream()
                .map(typeManager::getType)
                .collect(toImmutableList());
        List<MethodHandle> casts = types.stream()
                .map(type -> typeManager.resolveOperator(OperatorType.CAST, ImmutableList.of(VARCHAR, type)))
                .collect(toImmutableList());

        return new RowTransformTableFunction(new SplitColumnTransform(handle.getSplitColumnIndex(), utf8Slice(handle.getDelimiter()), types, casts));
    }

    private static <T> T getParameter(Map<String, Object> arguments, String name, Class<T> expectedType, final String sqlType)
    {
        Object value = arguments.get(name);
        if (value == null) {
            throw new PrestoException(SYNTAX_ERROR, "Parameter '" + name + "' is required");
        }
        if (!expectedType.isInstance(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter '" + name + "' must be a " + sqlType);
        }
        return expectedType.cast(value);
    }

    private static List<ColumnMetadata> getDescriptorParameter(Map<String, Object> arguments, String name)
    {
        List<?> input = getParameter(arguments, name, List.class, "descriptor");
        return input.stream()
                .map(ColumnMetadata.class::cast)
                .collect(toImmutableList());
    }

    public static class SplitColumnFunctionHandle
    {
        private final int splitColumnIndex;
        private final String delimiter;
        private final List<TypeSignature> outputTypes;

        @JsonCreator
        public SplitColumnFunctionHandle(
                @JsonProperty("splitColumnIndex") int splitColumnIndex,
                @JsonProperty("delimiter") String delimiter,
                @JsonProperty("outputTypes") List<TypeSignature> outputTypes)
        {
            this.splitColumnIndex = splitColumnIndex;
            this.delimiter = requireNonNull(delimiter, "delimiter is null");
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        }

        @JsonProperty
        public int getSplitColumnIndex()
        {
            return splitColumnIndex;
        }

        @JsonProperty
        public String getDelimiter()
        {
            return delimiter;
        }

        @JsonProperty
        public List<TypeSignature> getOutputTypes()
        {
            return outputTypes;
        }
    }
}
