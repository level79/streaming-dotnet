using System.Linq.Expressions;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using NodaTime;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

namespace Level79.Common.EventStreaming.Production.Serialization;

public class BinaryLocalDateSerializerBuilderCase : SerializerBuilderCase, IBinarySerializerBuilderCase
{
    private static readonly LocalDate Epoch = new(1970, 1, 1);

    public BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema,
        BinarySerializerBuilderContext context)
    {
        if (schema.LogicalType is DateLogicalType && type == typeof(LocalDate))
        {
            if (schema is not IntSchema)
            {
                throw new UnsupportedSchemaException(schema);
            }

            Expression expression;

            try
            {
                expression = BuildConversion(value, typeof(LocalDate));
            }
            catch (InvalidOperationException exception)
            {
                throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
            }

            var periodDays = typeof(Period).GetMethod(nameof(Period.DaysBetween));

            var writeInteger = typeof(BinaryWriter)
                .GetMethod(nameof(BinaryWriter.WriteInteger), new[] {typeof(int)});

            // return writer.WriteInteger(value.Minus(Epoch).Days);
            return BinarySerializerBuilderCaseResult.FromExpression(
                Expression.Call(
                    context.Writer,
                    writeInteger,
                    Expression.Call(
                        null,
                        periodDays,
                        Expression.Constant(Epoch),
                        expression
                    )
                )
            );
        }
        else
        {
            return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema,
                $"{nameof(BinaryTimestampSerializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
        }
    }
}