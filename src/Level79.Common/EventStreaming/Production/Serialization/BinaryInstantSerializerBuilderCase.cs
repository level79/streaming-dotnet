using System.Linq.Expressions;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using NodaTime;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace Level79.Common.EventStreaming.Production.Serialization;

public class BinaryInstantSerializerBuilderCase : TimestampSerializerBuilderCase, IBinarySerializerBuilderCase
{
    public BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema,
        BinarySerializerBuilderContext context)
    {
        if (schema.LogicalType is TimestampLogicalType && type == typeof(Instant))
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