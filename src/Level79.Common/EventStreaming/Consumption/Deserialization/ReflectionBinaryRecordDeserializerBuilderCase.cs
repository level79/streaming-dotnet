using System.Reflection;
using Chr.Avro.Abstract;
using Chr.Avro.Serialization;
using Level79.Common.Reflection;

namespace Level79.Common.EventStreaming.Consumption.Deserialization;

public class ReflectionBinaryRecordDeserializerBuilderCase : BinaryRecordDeserializerBuilderCase
{
    public ReflectionBinaryRecordDeserializerBuilderCase(IBinaryDeserializerBuilder builder) : base(builder,
        BindingFlags.Public | BindingFlags.Instance)
    {
    }

    public override BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema,
        BinaryDeserializerBuilderContext context)
    {
        var schemaType = schema switch
        {
            RecordSchema recordSchema => DetermineType(recordSchema.FullName),
            _ => type
        };
        return base.BuildExpression(schemaType, schema, context);
    }

    private static Type DetermineType(string recordSchemaFullName)
    {
        var determineType = AssemblyScanner.GetTypeByFullname(recordSchemaFullName,
                StringComparison.InvariantCultureIgnoreCase);

        return determineType switch
        {
            null => throw new ArgumentOutOfRangeException(nameof(recordSchemaFullName), recordSchemaFullName, null),
            _ => determineType
        };
    }
}