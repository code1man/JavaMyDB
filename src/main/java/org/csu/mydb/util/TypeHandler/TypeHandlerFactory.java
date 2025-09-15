package org.csu.mydb.util.TypeHandler;

// 类型处理器工厂（根据数据类型获取处理器）
public class TypeHandlerFactory {
    private static final java.util.Map<String, TypeHandler<?>> handlers = new java.util.HashMap<>();

    static {
        // 注册内置类型处理器
        handlers.put("INT", new IntTypeHandler());
        handlers.put("VARCHAR", new VarcharTypeHandler());
        // 可扩展其他类型（如LONG、DATE等）
    }

    @SuppressWarnings("unchecked")
    public static <K extends Comparable<K>> TypeHandler<K> getHandler(String dataType) {
        TypeHandler<?> handler = handlers.get(dataType.toUpperCase());
        if (handler == null) {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
        return (TypeHandler<K>) handler;
    }
}
