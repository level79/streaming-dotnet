using System.Linq.Expressions;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using NodaTime;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;

namespace Level79.Common.EventStreaming.Consumption.Deserialization;

public class LocalDateDeserializerBuilderCase : DeserializerBuilderCase, IBinaryDeserializerBuilderCase
{
    private static readonly LocalDate Epoch = new LocalDate(1970,1,1);

    public BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema,
        BinaryDeserializerBuilderContext context)
    {
        if (schema.LogicalType is DateLogicalType && type == typeof(LocalDate))
        {
            if (schema is not IntSchema)
            {
                throw new UnsupportedSchemaException(schema,
                    $"{nameof(TimestampLogicalType)} deserializers can only be built for {nameof(IntSchema)}s.");
            }
            
            var readInteger = typeof(BinaryReader)
                .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

            var expression = BuildConversion(Expression.Call(context.Reader, readInteger), typeof(int));
            
            var addPeriod = typeof(LocalDate).GetMethod(nameof(LocalDate.Add));
            var fromDays = typeof(Period).GetMethod(nameof(Period.FromDays));
            try
            {
                // return Epoch.AddTicks(value * factor);
                return BinaryDeserializerBuilderCaseResult.FromExpression(
                    BuildConversion(
                        Expression.Call(
                            null,
                            addPeriod,
                            Expression.Constant(Epoch), 
                            Expression.Call(null, fromDays, expression)),
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