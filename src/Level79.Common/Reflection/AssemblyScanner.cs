using System.Reflection;

namespace Level79.Common.Reflection;

public static class AssemblyScanner
{
    private static readonly AppDomain AppDomain = AppDomain.CurrentDomain;
    private static readonly Lazy<IEnumerable<Assembly>> Assemblies = 
        new(AppDomain.GetAssemblies().Where(p => !p.IsDynamic));
    private static readonly IEnumerable<Assembly> AllAssemblies = Assemblies.Value;

    private static readonly Lazy<IEnumerable<Assembly>> OurAssemblies = 
        new(AllAssemblies.Where(assembly => assembly.FullName?.Split('.')[0] == "Level79"));
    private static readonly IEnumerable<Assembly> AllOurAssemblies;

    static AssemblyScanner()
    {
        AllOurAssemblies = OurAssemblies.Value;
    }

    public static IEnumerable<Type> GetAllImplementingTypes<T>()
    {
        var type = typeof(T);
        if (!type.IsInterface) throw new ArgumentException("Type must be an interface");
        var exportedTypes = AllOurAssemblies.SelectMany(assembly => assembly.ExportedTypes);
        return exportedTypes.Where(t => t.GetInterfaces().Contains(type));
    }

    public static Type? GetTypeByFullname(string fullName, StringComparison comparisonType)
    {
        IEnumerable<Type?> exportedTypes = AllOurAssemblies.SelectMany(assembly => assembly.ExportedTypes);
        return exportedTypes.FirstOrDefault(type =>
            string.Equals(type?.FullName, fullName, comparisonType));
    }
}