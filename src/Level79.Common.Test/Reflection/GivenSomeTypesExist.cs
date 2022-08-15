using System;
using System.Linq;
using Level79.Common.EventStreaming;
using Level79.Common.Reflection;
using Shouldly;
using Xunit;

namespace Level79.Common.Test.Reflection;

public class GivenSomeTypesExist
{
    [Fact]
    public void WeCanFindThemByReflectionOnTheInterface()
    {
        var types = AssemblyScanner.GetAllImplementingTypes<IEvent>();
        var collection = types as Type[] ?? types.ToArray();
        collection.ShouldContain(typeof(HeartBeatEvent));
        collection.ShouldContain(typeof(EventInAnotherAssembly));
    }
    [Fact]
    public void WeCanFindThemByName()
    {
        var heartBeatEventType = typeof(HeartBeatEvent);
        var type = AssemblyScanner.GetTypeByFullname(heartBeatEventType.FullName!, StringComparison.InvariantCultureIgnoreCase);
        type.ShouldBe(heartBeatEventType);
    }
}