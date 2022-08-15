using System.Linq.Expressions;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using NodaTime;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;

namespace Level79.Common.EventStreaming.Consumption;

public class InstantTimestampDeserializerBuilderCase : TimestampDeserializerBuilderCase, IBinaryDeserializerBuilderCase
{
    /// <summary>
    /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="TimestampLogicalType" />.
    /// </summary>
    /// <returns>
    /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
    /// has a <see cref="TimestampLogicalType" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
    /// otherwise.
    /// </returns>
    /// <exception cref="UnsupportedSchemaException">
    /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
    /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
    /// or a <see cref="MillisecondTimestampLogicalType" />.
    /// </exception>
    /// <exception cref="UnsupportedTypeException">
    /// Thrown when <see cref="DateTime" /> cannot be converted to <paramref name="type" />.
    /// </exception>
    /// <inheritdoc />
    public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema,
        BinaryDeserializerBuilderContext context)
    {
        if (schema.LogicalType is TimestampLogicalType)
        {
            if (schema is not LongSchema)
            {
                throw new UnsupportedSchemaException(schema,
                    $"{nameof(TimestampLogicalType)} deserializers can only be built for {nameof(LongSchema)}s.");
            }

            var factor = schema.LogicalType switch
            {
                MicrosecondTimestampLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                MillisecondTimestampLogicalType => TimeSpan.TicksPerMillisecond,
                _ => throw new UnsupportedSchemaException(schema,
                    $"{schema.LogicalType} is not a supported {nameof(TimestampLogicalType)}."),
            };

            var readInteger = typeof(BinaryReader)
                .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

            Expression expression = Expression.Call(context.Reader, readInteger);

            var instantFromTicks = typeof(Instant)
                .GetMethod(nameof(Instant.FromUnixTimeTicks), new[] {typeof(long)});

            try
            {
                // return Epoch.AddTicks(value * factor);
                return BinaryDeserializerBuilderCaseResult.FromExpression(
                    BuildConversion(
                        Expression.Call(
                            null,
                            instantFromTicks,
                            Expression.Multiply(expression, Expression.Constant(factor))),
                        type));
            }
            catch (InvalidOperationException exception)
            {
                throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
            }
        }
        else
        {
            return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema,
                $"{nameof(BinaryTimestampDeserializerBuilderCase)} can only be applied to schemas with a {nameof(TimestampLogicalType)}."));
        }
    }
}