using Chr.Avro;
using Chr.Avro.Abstract;
using NodaTime;

namespace Level79.Common.EventStreaming.Production.SchemaBuilderCases;

public class LocalDateSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
{
    public TemporalBehavior TemporalBehavior { get; set; }

    public LocalDateSchemaBuilderCase(TemporalBehavior temporalBehavior)
    {
        TemporalBehavior = temporalBehavior;
    }

    public SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
    {
        if (type == typeof(LocalDate))
        {
            Schema dateSchema = TemporalBehavior switch
            {
                TemporalBehavior.Iso8601 => new StringSchema(),
                _ => new IntSchema()
                {
                    LogicalType = new DateLogicalType()
                }
            };

            try
            {
                context.Schemas.Add(type, dateSchema);
            }
            catch (ArgumentException exception)
            {
                throw new InvalidOperationException(
                    $"A schema for {type} already exists on the schema builder context.", exception);
            }

            return SchemaBuilderCaseResult.FromSchema(dateSchema);
        }
        else
        {
            return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type,
                $"{nameof(TimestampSchemaBuilderCase)} can only be applied to the {nameof(DateTime)} and {nameof(DateTimeOffset)} types."));
        }
    }
}

public class InstantSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
{
    public InstantSchemaBuilderCase(TemporalBehavior temporalBehavior)
    {
        TemporalBehavior = temporalBehavior;
    }

    public TemporalBehavior TemporalBehavior { get; }

    public SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
    {
        if (type == typeof(Instant))
        {
            Schema timestampSchema = TemporalBehavior switch
            {
                TemporalBehavior.EpochMicroseconds => new LongSchema()
                {
                    LogicalType = new MicrosecondTimestampLogicalType(),
                },
                TemporalBehavior.EpochMilliseconds => new LongSchema()
                {
                    LogicalType = new MillisecondTimestampLogicalType(),
                },
                TemporalBehavior.Iso8601 => new StringSchema(),
                _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior)),
            };

            try
            {
                context.Schemas.Add(type, timestampSchema);
            }
            catch (ArgumentException exception)
            {
                throw new InvalidOperationException(
                    $"A schema for {type} already exists on the schema builder context.", exception);
            }

            return SchemaBuilderCaseResult.FromSchema(timestampSchema);
        }
        else
        {
            return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type,
                $"{nameof(TimestampSchemaBuilderCase)} can only be applied to the {nameof(DateTime)} and {nameof(DateTimeOffset)} types."));
        }
    }
}