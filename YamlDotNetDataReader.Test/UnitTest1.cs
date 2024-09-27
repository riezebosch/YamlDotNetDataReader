using System.Data;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using YamlDotNet.Serialization;

namespace YamlDotNetDataReader.Tests;

public class UnitTest1
{
    [Theory]
    [InlineData("ptUICOMPONENTPROPERTYVALUE")]
    [InlineData("vtItem")]
    [InlineData("vtDocument")]
    public async Task ToYaml(string name)
    {
        await using var connection =
            new SqlConnection(
                @"Server=.\sqlexpress;Database=LFBase_CABE;Integrated Security=SSPI;TrustServerCertificate=true");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {name}";

        var reader = await command.ExecuteReaderAsync();

        await using var stream = File.Create($"{name}.yaml");
        await using var writer = new StreamWriter(stream);
        Factory.Serializer().Serialize(writer, reader);
    }

    [Theory]
    [InlineData("ptUICOMPONENTPROPERTYVALUE")]
    [InlineData("vtItem")]
    [InlineData("vtDocument")]
    public async Task FromYaml(string name)
    {
        await using var connection =
            new SqlConnection(
                @"Server=.\sqlexpress;Database=LFBase_CABE;Integrated Security=SSPI;TrustServerCertificate=true");
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = @"EXEC sp_MSforeachtable ""ALTER TABLE ? NOCHECK CONSTRAINT all""";
        await command.ExecuteNonQueryAsync();

        command.CommandText = $"DELETE FROM {name}";
        await command.ExecuteNonQueryAsync();

        await using var stream = File.OpenRead($"{name}.yaml");
        using var reader = new StreamReader(stream);
        var data = Factory.Deserializer().Deserialize<IDataReader>(reader);

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

    [Fact]
    public void FromYamlBinary()
    {
        var data = Factory
            .Deserializer()
            .Deserialize<IDataReader>("""
                                      - Data: !!binary R0lGODlhIAAgAPf/AEtmqqdBG3eaw3Jyc6Y1FESL53il5nOs84ilxfHububl4ipUt3WIi7W2r93ETICaybW8yCh43n5+f8O9pDxTmTY4Txxize7na+vXUzSE5XeNxuLNczdnyfX0hPLs4suZU9/GaBx341uc7Pj49/7+/8rO1iNGpLu7vWqVy8XFxWib5BJRxf/xh7rE0+7u6yx64eTf06FOH469++PLUOTSZKllNqytq9zYzKNRKlSW63eCqPzzazqA0kiK3YO1+Zmt2WSj76Wz06nS+fb3chgsasrKyqXM/+nYfCY3comgvClq0/PugsPCsjmE42d7u/Hx8vT09MPK5FWa84OevYqKikxQbOvu9urfa2up8cPCu/7rhXuz9PXx6v//nXWc0zdFe9u7ZBckULK91YWViJKqxdrVxhY3l0uU652xxezv9Wyb3ZTB/2SX1TSH6BYyha27zPf5/tTRxVqW4tLS0i1NqbCzszZ84T1z1Q4fUiV85Qssi///juTQhNzc3N/e1jt53f//mv//oJ5PJi2A4z6M6Kqxs5iYmNfVzOzbVtTOvaFJHHmYu//1iQMdZoau7VaL3bHU/0h/wdGtYAk0k//4iJnE/6I+GTR63uzdWEh+2ebm7Ojo4vn5+//+90tfmPX0k/n3lfv6l+Hl9MbGrbKASO7le///hIuMk0aT9djWzVaV4rBaMpC//lWZ7FiU7n+w94i6/GCOwmeI06ddNa9KJbJVLdbX2LV2RDeI6+fRUYy98urWXoePfn6Ak+jZafTjfurVZ2R7w7u6sPHlXvniVv//WZukqrGsnVeMyl6W3Pbig/nmg83EmJKhtG+AiImav2+Cr97HgaYtEPLdgkZpx1lqjVx+yi5Xr+zn23Fzhse/nJDB8y5cwKDH//T2+wcukjB3yvj5h/v+iYms3/v/jI2Wr2Rmevb29Ovs8ml3ocG+seDg2PDbcezo36W0wqu8w/r9maenqKSrrunVhO7Zg9mxYdq/atPCjI+479/Mhu3v9YudsG6h6Nq/WtSmWv///yH5BAEAAP8ALAAAAAAgACAAAAj/AP8JHEiwoMGDCBMqXMiwocOHD61InEgxTRp9GK2IQueN4TNrsnTwGjOGgcmTJ51VA0At3QiFAGSgmlAsFwabuRDlnIFpxjBiZFyZKQLThw8mDmIQUMrUkiJLARTNePfKTB2F0F5ByjJsyAWvYL9+TbAjSzcz8RRq4GcEwo4Lvq7Q8EWjLgga/WjsYlLJhCG1Krq9AVZDmiAChw/jsLS436g1Jk6pfVQpCLsOpTos6dBBXDhy4faQMzWB1YJeCstlkvHj1xIQR8BsqGfPn6QPuMFog8Utm0IId2CNO0KKAI4AOGjNolWj1q1VIJj54GBOYRQOWwzwCfUJFDxAXboE/xo/HtC9VxyqWOd2gF80SoxYaNGyTNk0evP45It27EB6hSUsAAQQSRjTTBJJPPCAF16ooYYKEAKBxR1fcJLQE9eIIIIUUuTgoRxyJMMGGyig0EMTBQBBIRQJcbJADq2oso88NthQRwPCCKMOE+pEEkIErfyBhAsJwbFAD2f0IEwhUyxSSBlxRBlHIrG0kcEZfxChQIt0oNhDHQLggksSNxySypllCHBGAQXYQUQfCZHgiR2ENFEHAhlkMAUMfvS5DgwIANFKE5foMYdCTkTQxCDukDHIIAJgo8Ckm7SDxhYHNPGCHikoFIwSg0SAhjsR5IGCBy6k6gIXb2yjSxMR6INxAqIWDBICGRCEEAIyXJzj6widtCCEEITk8Y0NCumwQh4hIFACODwI0AkJ1JIAhxj4OKLECo0gm5AtZqwQAgqaiCFGED884AQAFCBBBB54VFDBAFsaRMJAJ7gxiRvuhhGGvPNKcMoJKczxxBMOpUAFFYYUUUQfnIxgIUQUV2zxxRUHBAA7
                                      """);

        data.Read().Should().BeTrue();
        data["Data"].Should().BeOfType<byte[]>();
    }
}