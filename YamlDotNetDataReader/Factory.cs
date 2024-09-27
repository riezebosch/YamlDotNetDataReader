using YamlDotNet.Serialization;

namespace YamlDotNetDataReader;

public static class Factory
{
    public static class Builder
    {
        public static SerializerBuilder Serializer() =>
            new SerializerBuilder()
                .WithTypeConverter(new DataReaderTypeConverter())
                .WithTypeConverter(new ByteArrayConverter());

        public static DeserializerBuilder Deserializer() =>
            new DeserializerBuilder()
                .WithTypeConverter(new DataReaderTypeConverter())
                .WithTypeConverter(new ByteArrayConverter())
                .WithTagMapping(ByteArrayConverter.BinaryTag, typeof(byte[]));
    }

    public static ISerializer Serializer() => Builder.Serializer().Build();
    public static IDeserializer Deserializer() => Builder.Deserializer().Build();
}