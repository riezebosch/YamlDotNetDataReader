# YamlDotNetDataReader

A `YamlDotNet` type converter to turn `yaml` into `sql` and vice versa.

## Export data from `sql` into `yaml`

```csharp
await using var connection = new SqlConnection("...");
await connection.OpenAsync();

await using var command = connection.CreateCommand();
command.CommandText = $"SELECT * FROM ...";

var reader = await command.ExecuteReaderAsync();

var serializer = new SerializerBuilder()
    .WithTypeConverter(new DataReaderTypeConverter())
    .Build();

await using var stream = File.Create("output.yaml");
await using var writer = new StreamWriter(stream);
serializer.Serialize(writer, reader);
```

## Import data from `yaml` into `sql`

```csharp
var deserializer = new DeserializerBuilder()
    .WithTypeConverter(new DataReaderTypeConverter())
    .Build();

await using var stream = File.OpenRead("output.yaml");
using var reader = new StreamReader(stream);
var data = deserializer.Deserialize<IDataReader>(reader);

var copy = new SqlBulkCopy("...");
copy.DestinationTableName = "...";

// optional: when null values are omitted and values are not in strict order
Enumerable
    .Range(0, data.FieldCount)
    .Select(data.GetName)
    .ToList()
    .ForEach(m => copy.ColumnMappings.Add(m, m));

await copy.WriteToServerAsync(data);
```