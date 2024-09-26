using System.Data;
using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace YamlDotNetDataReader;

public class DataReaderTypeConverter(bool exportNullValues = false) : IYamlTypeConverter
{
    bool IYamlTypeConverter.Accepts(Type type) => typeof(IDataReader).IsAssignableFrom(type);

    object IYamlTypeConverter.ReadYaml(IParser parser, Type type, ObjectDeserializer rootDeserializer)
    {
        var table = new DataTable();

        parser.Consume<SequenceStart>();
        while (parser.TryConsume<MappingStart>(out _))
        {
            var row = table.NewRow();
            while (parser.TryConsume<Scalar>(out var field))
            {
                if (!table.Columns.Contains(field.Value))
                {
                    table.Columns.Add(field.Value);
                }

                row[field.Value] = rootDeserializer(typeof(object));
            }

            table.Rows.Add(row);
            parser.Consume<MappingEnd>();
        }

        parser.Consume<SequenceEnd>();
        return table.CreateDataReader();
    }

    void IYamlTypeConverter.WriteYaml(IEmitter emitter, object? value, Type type, ObjectSerializer serializer)
    {
        var reader = (IDataReader)value!;
        emitter.Emit(new SequenceStart(AnchorName.Empty, TagName.Empty, true, SequenceStyle.Any));
        while (reader.Read())
        {
            emitter.Emit(new MappingStart(AnchorName.Empty, TagName.Empty, true, MappingStyle.Block));
            for (var field = 0; field < reader.FieldCount; field++)
            {
                if (!exportNullValues && reader.IsDBNull(field)) continue;
                
                emitter.Emit(new Scalar(reader.GetName(field)));
                serializer(!reader.IsDBNull(field) ? reader[field] : null, reader.GetFieldType(field));
            }
            emitter.Emit(new MappingEnd());
        }
        emitter.Emit(new SequenceEnd());
    }
}