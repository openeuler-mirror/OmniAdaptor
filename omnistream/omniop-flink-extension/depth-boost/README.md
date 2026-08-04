# 特性指南

## 1. 特性概述

本特性基于 Java Agent 技术实现业务方法的动态替换与加速。

应用启动时加载 Java Agent。Agent 会读取 JVM 参数中配置的目标类和目标方法，定位业务代码中符合替换规则的方法调用，并将其替换为项目提供的加速实现。

以 `String.replaceAll` 为例，原业务代码如下：

```java
String result = input.replaceAll(regex, replacement);
```

当目标类和目标方法命中配置规则后，Agent 会在目标业务方法中将此类调用替换为 `ReplaceHelper.replaceAllFast` 方法。`ReplaceHelper` 负责匹配加速规则，并通过 JNI 调用 Native 动态库，使用基于 SVE 的向量化实现完成字符串处理，从而提升执行效率。

整个替换过程在应用启动阶段自动完成，无需手工修改业务代码。

## 2. 特性使能

启动应用前，请确认以下文件已经部署到目标服务器：

| 文件 | 示例路径 | 说明 |
| --- | --- | --- |
| Java Agent JAR | `/opt/flink-tnel-0.1-SNAPSHOT.jar` | 用于加载 Agent 并执行字节码增强 |
| Native 动态库 | `/opt/libregex.so` | 提供基于 SVE 向量化的 Native 加速实现 |

同时需要确认：

- 应用进程对上述文件具有读取权限；
- `libregex.so` 的依赖库能够被系统正确解析；
- Native 实现、CPU 指令集和 JVM 版本满足项目运行要求；
- 已根据实际业务代码确定需要增强的目标类和目标方法。


### 2.1 配置 Native 动态库加载路径

通过 JVM 参数 `java.library.path` 指定 `libregex.so` 所在目录。


```bash
-Djava.library.path=/opt/
```

> `java.library.path` 应填写动态库所在的目录，而不是 `.so` 文件的完整路径。

### 2.2 加载 Java Agent

通过 `-javaagent` 参数指定 Java Agent JAR：

```bash
-javaagent:/opt/flink-tnel-0.1-SNAPSHOT.jar
```

### 2.3 配置目标类和目标方法

通过 JVM 系统属性指定需要进行字节码增强的目标类和目标方法（参考配置文档 [配置手册](AGENT_CONFIG.md) ）：

```bash
-Dagent.targets=com.example.classA
-Dagent.replaceMethods=com.example.methodA
```

示例中的类名和方法名仅用于说明。实际使用时，请根据业务代码填写正确的完整类名和方法名。

### 2.4 完整启动示例

将 Native 库路径、Agent 路径和替换规则一并加入应用的 JVM 启动参数：

```bash
  -Djava.library.path=/opt/ 
  -javaagent:/opt/flink-tnel-0.1-SNAPSHOT.jar 
  -Dagent.targets=com.example.classA 
  -Dagent.replaceMethods=com.example.methodA 
  -jar application.jar
```

## 3. 核心类说明

### 3.1 `MethodReplacerAgent`

Java Agent 的入口类，负责读取配置、加载 Native 动态库，并使用 Byte Buddy 替换目标方法内的指定调用。以 `replaceAll` 为例，Agent 会将目标方法中的 `String.replaceAll` 调用替换为 `ReplaceHelper.replaceAllFast`。

### 3.2 `Helper` 加速类

`Helper` 类用于承接替换后的 Java 方法调用，并通过 Native 实现完成加速。以 `ReplaceHelper` 为例，其 `replaceAllFast` 方法会通过 JNI 调用 `libregex.so` 中的 Native 方法。

## 4. 编译构建

在项目上层目录 `omni-flink-bundle` 中执行以下命令：

```bash
mvn -Pdepth-boost -DskipTests clean package
```

构建成功后，生成的 Agent JAR 位于：

```text
depth-boost/target/flink-tnel-0.1-SNAPSHOT.jar
```