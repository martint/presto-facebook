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
import com.facebook.presto.spi.function.PolymorphicTableFunction;
import com.facebook.presto.spi.function.TableFunctionImplementation;
import com.facebook.presto.spi.function.TableFunctionDescriptor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ApproximateMostFrequentTableFunctionFactory
        implements PolymorphicTableFunction
{
    private static final JsonCodec<ApproximateMostFrequentTableFunctionHandle> CODEC = JsonCodec.jsonCodec(ApproximateMostFrequentTableFunctionHandle.class);
    private final TypeManager typeManager;

    public ApproximateMostFrequentTableFunctionFactory(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return "approx_most_frequent";
    }

    @Override
    public TableFunctionDescriptor specialize(Map<String, Object> arguments)
    {
        List<ColumnMetadata> inputs = getDescriptorParameter(arguments, "input");

        ApproximateMostFrequentTableFunctionHandle handle = new ApproximateMostFrequentTableFunctionHandle(
                getParameter(arguments, "number", Integer.class, "integer"),
                getParameter(arguments, "error", Double.class, "double"),
                inputs.stream()
                        .map(type -> type.getType().getTypeSignature())
                        .collect(toImmutableList()));

        return new TableFunctionDescriptor(
                CODEC.toJsonBytes(handle),
                IntStream.range(0, inputs.size()).boxed().collect(toImmutableList()),
                ImmutableList.<ColumnMetadata>builder()
                        .addAll(inputs)
                        .add(new ColumnMetadata("count", BIGINT))
                        .add(new ColumnMetadata("error", BIGINT))
                        .build());
    }

    @Override
    public TableFunctionImplementation getInstance(byte[] handleJson)
    {
        ApproximateMostFrequentTableFunctionHandle handle = CODEC.fromJson(handleJson);

        List<Type> types = handle.getTypes().stream()
                .map(typeManager::getType)
                .collect(toImmutableList());

        return new ApproximateMostFrequentTableFunction(types, handle.getNumber(), handle.getError());
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

    public static class ApproximateMostFrequentTableFunctionHandle
    {
        private final int number;
        private final double error;
        private final List<TypeSignature> types;

        @JsonCreator
        public ApproximateMostFrequentTableFunctionHandle(
                @JsonProperty("number") int number,
                @JsonProperty("error") double error,
                @JsonProperty("types") List<TypeSignature> types)
        {
            this.number = number;
            this.error = error;
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @JsonProperty
        public int getNumber()
        {
            return number;
        }

        @JsonProperty
        public double getError()
        {
            return error;
        }

        @JsonProperty
        public List<TypeSignature> getTypes()
        {
            return types;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("number", number)
                    .add("error", error)
                    .add("types", types)
                    .toString();
        }
    }
}
