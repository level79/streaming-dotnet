using System.Linq.Expressions;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using NodaTime;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace Level79.Common.EventStreaming.Production;

public class BinaryInstantSerializerBuilderCase : TimestampSerializerBuilderCase, IBinarySerializerBuilderCase
{
    /// <summary>
    /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="TimestampLogicalType" />.
    /// </summary>
    /// <returns>
    /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
    /// has a <see cref="TimestampLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
    /// otherwise.
    /// </returns>
    /// <exception cref="UnsupportedSchemaException">
    /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
    /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
    /// or a <see cref="MillisecondTimestampLogicalType" />.
    /// </exception>
    /// <exception cref="UnsupportedTypeException">
    /// Thrown when <paramref name="type" /> cannot be converted to <see cref="DateTimeOffset" />.
    /// </exception>
    /// <inheritdoc />
    public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema,
        BinarySerializerBuilderContext context)
    {
        if (schema.LogicalType is TimestampLogicalType)
        {
            if (schema is not LongSchema)
            {
                throw new UnsupportedSchemaException(schema);
            }

            Expression expression;

            try
            {
                expression = BuildConversion(value, typeof(Instant));
            }
            catch (InvalidOperationException exception)
            {
                throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
            }

            var factor = schema.LogicalType switch
            {
                MicrosecondTimestampLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                MillisecondTimestampLogicalType => TimeSpan.TicksPerMillisecond,
                _ => throw new UnsupportedSchemaException(schema,
                    $"{schema.LogicalType} is not a supported {nameof(TimestampLogicalType)}."),
            };

            var utcTicks = typeof(Instant)
                .GetMethod(nameof(Instant.ToUnixTimeTicks));

            var writeInteger = typeof(BinaryWriter)
                .GetMethod(nameof(BinaryWriter.WriteInteger), new[] {typeof(long)});

            // return writer.WriteInteger(value.UtcTicks / factor);
            return BinarySerializerBuilderCaseResult.FromExpression(
                Expression.Call(
                    context.Writer,
                    writeInteger,
                    Expression.Divide(
                        Expression.Call(expression, utcTicks),
                        Expression.Constant(factor))));
        }
        else
        {
            return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema,
                $"{nameof(BinaryTimestampSerializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
        }
    }
}