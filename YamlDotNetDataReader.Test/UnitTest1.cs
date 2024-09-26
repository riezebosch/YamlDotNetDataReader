using System.Data;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using YamlDotNet.Serialization;

namespace YamlDotNetDataReader.Tests;

public class UnitTest1
{
    [Fact]
    public async Task ToYaml()
    {
        var name = "dbo.ptUICOMPONENTPROPERTYVALUE";
        await using var connection =
            new SqlConnection(
                @"Server=.\sqlexpress;Database=LFBase_CABE;Integrated Security=SSPI;TrustServerCertificate=true");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {name}";

        var reader = await command.ExecuteReaderAsync();
        
        var serializer = new SerializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter())
            .Build();

        await using var stream = File.Create("output.yaml");
        await using var writer = new StreamWriter(stream);
        serializer.Serialize(writer, reader);
    }
    
    [Fact]
    public async Task FromYaml()
    {
        var name = "ptUICOMPONENTPROPERTYVALUE";
        await using var connection =
            new SqlConnection(
                @"Server=.\sqlexpress;Database=LFBase_CABE;Integrated Security=SSPI;TrustServerCertificate=true");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {name}";
        await command.ExecuteNonQueryAsync();
        
        var deserializer = new DeserializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter())
            .Build();
        
        await using var stream = File.OpenRead("output.yaml");
        using var reader = new StreamReader(stream);
        var data = deserializer.Deserialize<IDataReader>(reader);

        var copy = new SqlBulkCopy(connection.ConnectionString,
            SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.UseInternalTransaction);
        copy.DestinationTableName = name;

        Enumerable
            .Range(0, data.FieldCount)
            .Select(data.GetName)
            .ToList()
            .ForEach(m => copy.ColumnMappings.Add(m, m));
        
        await copy.WriteToServerAsync(data);
    }
    
    [Fact]
    public void FromYamlNull()
    {
        var deserializer = new DeserializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter())
            .Build();
        
        var data = deserializer.Deserialize<IDataReader>("""
                                                         - a: ~
                                                           b:
                                                           c: null
                                                           d: ''
                                                           e: 'x'
                                                         """);

        data.Read().Should().BeTrue();
        data["a"].Should().Be(DBNull.Value);
        data["b"].Should().Be(DBNull.Value);
        data["c"].Should().Be(DBNull.Value);
        data["d"].Should().Be(string.Empty);
        data["e"].Should().Be("x");
    }
    
    [Fact]
    public void FromYamlEmpty()
    {
        var deserializer = new DeserializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter())
            .Build();
        
        var data = deserializer.Deserialize<IDataReader>("");
        data.Should().BeNull();
    }

    [Fact]
    public void ToYamlNullSkip()
    {
        var serializer = new SerializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter())
            .Build();

        var data = new DataTable();
        data.Columns.Add("c1");
        data.Columns.Add("c2");
        data.Rows.Add("a", null);
        var result = serializer.Serialize(data.CreateDataReader());

        result.Should().Be("""
                           - c1: a
                           
                           """);
    }
    
    [Fact]
    public void ToYamlWithNewLines()
    {
        var serializer = new SerializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter())
            .Build();

        var data = new DataTable();
        data.Columns.Add("x");
        data.Rows.Add("""
                       hello
                       this
                       is
                       some
                       interesting
                       conversation
                       """);
        var result = serializer.Serialize(data.CreateDataReader());

        result.Should().Be("""
                           - x: >-
                               hello

                               this

                               is

                               some

                               interesting

                               conversation
                           
                           """);
    }

    [Fact]
    public void ToYamlNullInclude()
    {
        var serializer = new SerializerBuilder()
            .WithTypeConverter(new DataReaderTypeConverter(true))
            .Build();

        var data = new DataTable();
        data.Columns.Add("c1");
        data.Columns.Add("c2");
        data.Rows.Add("a", null);
        var result = serializer.Serialize(data.CreateDataReader());

        result.Should().Be("""
                           - c1: a
                             c2: 
                           
                           """);
    }
}