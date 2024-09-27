using YamlDotNet.Core;
using YamlDotNet.Core.Events;
using YamlDotNet.Serialization;

namespace YamlDotNetDataReader;

/// <summary>
/// source: https://stackoverflow.com/a/37862311/129269
/// </summary>
public class ByteArrayConverter : IYamlTypeConverter
{
    public static readonly TagName BinaryTag = "tag:yaml.org,2002:binary";
    
    bool IYamlTypeConverter.Accepts(Type type) => type == typeof(byte[]);

    object IYamlTypeConverter.ReadYaml(IParser parser, Type type, ObjectDeserializer rootDeserializer) => 
        Convert.FromBase64String(parser.Consume<Scalar>().Value);

    void IYamlTypeConverter.WriteYaml(IEmitter emitter, object? value, Type type, ObjectSerializer serializer) =>
        emitter.Emit(new Scalar(
            null,
            BinaryTag,
            Convert.ToBase64String((byte[])value!),
            ScalarStyle.Plain,
            false,
            false
        ));
}